import duckdb
import pandas
import polars
import cooler
import numpy
from typing import *
from hich.scool import ScoolInput
from pairs import PairsFile, PairsHeader

class PairsScoolInput(ScoolInput):
    """Interface between 4DN .pairs format to cooler.create_scool
    """

    def __init__(self):
        self.conn = None

    def try_parse(self, obj: Union[PairsFile, str], config: Dict[str, Any]) -> bool:
        """If returns True, the ScoolInput stores and can parse obj
        
        Arguments:
            obj (Union[PairsFile, str]): Either a PairsFile object or a path to a valid .pairs file
            config (Dict[str, Any]):
                Required
                    "resolution": a positive integer
                Optional
                    "cell_name_col" -- column name that stores the cell names to be parsed
                    "regexp_extract_pattern" -- DuckDB regexp_extract pattern argumnet
                    "regexp_extract_group" -- DuckDB regexp_extract group argumnet
                    "bins" -- pandas DataFrame to use for bins
        """
        try:
            self.filename = obj
            self.pairs = PairsFile(self.filename)
            assert config.get("resolution") >= 1 and isinstance(config.get("resolution"), int)
            
            self.config = config
            self.skip = len(self.pairs.header.split("\n"))
            return self.pairs.header.valid_header()
        except:
            return False

    def bins(
            self, 
            first: bool = True
            ) -> Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]:
        """Generate bins value for cooler.create_scool

        Arguments:
            first (bool): If True, only the first DataFrame in the dictionary (keyed by cell names) will have 'chrom', 'start', and 'end' columns; others will use `None` as placeholders. If False, all DataFrames will use placeholders. This minimizes memory use while meeting `create_scool` requirements for these columns.

        Returns:
            Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]: A single DataFrame with shared bins for all cells, or a dictionary mapping cell names to DataFrames with cell-specific bins (e.g., including 'weights' columns).
        """
        
        # Check if we already have the bins to use
        if isinstance(self.config.get("bins"), pandas.DataFrame):
            self.bins_df = self.config["bins"].copy()
            if not first:
                self.config["bins"]["chrom"] = None
                self.config["bins"]["start"] = None
                self.config["bins"]["end"] = None
            return self.config["bins"]
        
        # We will return an empty dataframe if the common bins columns aren't needed (first = False)
        self.bins_df = {"chrom": [], "start": [], "end": []}

        # Iterate through chromsizes in lexicographic order of chromname

        chromnames = sorted(list(self.header.chromsizes.keys()))
        for chromname in chromnames:
            # Extract the chromsize
            size = self.pairs.chromsizes[chromname]

            # Get bin partitions for the chromosome
            self.bins_df["start"] += numpy.arange(0, size, self.uniform_resolution).tolist()
            self.bins_df["end"] += (numpy.arange(0, size, self.uniform_resolution) + self.uniform_resolution).tolist()
        
        # Store for use in cell_name_pixels_dict
        self.bins_df = pandas.DataFrame(self.bins_df)

        # Drop unnecessary columns if this is not the first bins dict
        bins_df = self.bins_df.copy()
        if not first:
            bins_df["chrom"] = None
            bins_df["start"] = None
            bins_df["end"] = None

        return bins_df

    def cell_name_pixels_dict(self) -> Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]:
        """Generate cell_name_pixels_dict value for cooler.create_scool

        Returns:
            Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]: Maps cell group names to either DataFrames or iterables of DataFrames.
        """
        # We need to retrieve the cell names from the pairs file at this point in order to create the dictionary
        # but we don't want to create the entire pixels table until we're actively ready to select from it



    def iter_pixels(self, cell_name: str):
        if not self.conn:
            self.conn = duckdb.connect(":memory:")
            bins = self.bins.copy()
            bins["index"] = bins.index

            if (
                self.config.get("cell_name_col") 
                and self.config.get("regexp_extract_pattern") 
                and isinstance(self.config.get("regexp_extract_group"), int)
                ):
                self.create_pixels_table_singlecell()
        
        yield self.conn.execute("SELECT bin1_id, bin2_id, count FROM pixels WHERE cell_name = $cell_name", {"cell_name": cell_name}).df()

    def create_pixels_table_singlecell(self):
        self.pairs: PairsFile
        self.cell_names = self.conn.execute(
            f"""
DROP TABLE IF EXISTS pixels;

-- Count pairs at given binning resolution
-- Subtract 1 from pos1 and pos2 to match cooler cload pairs output
CREATE TABLE pixels AS

WITH pixels_cte AS (
    SELECT
        regexp_extract({self.config.get("cell_name_col")}, '$regexp_extract_pattern', $regexp_extract_group) AS barcode,
        chrom1,
        CAST(FLOOR((pos1-1)/$res) AS INTEGER)*$res AS start1,
        chrom2,
        CAST(FLOOR((pos2-1)/$res) AS INTEGER)*$res AS start2,
        COUNT(*) AS count
    FROM read_csv('$filename', skip=$skip, names=$names)
    GROUP BY barcode, chrom1, chrom2, start1, start2
)
SELECT barcode, bins1.index AS bin1_id, bins2.index AS bin2_id, count
FROM pixels_cte
JOIN bins AS bins1
ON pixels_cte.chrom1 = bins1.chrom AND pixels_cte.start1 = bins1.start
JOIN bins AS bins2
ON pixels_cte.chrom2 = bins2.chrom AND pixels_cte.start2 = bins2.start;

SELECT DISTINCT cell_name FROM pixels;
            """,
            {
                "regexp_extract_pattern": self.config.get("regexp_extract_pattern"),
                "regexp_extract_group": self.config.get("regexp_extract_group"),
                "res": self.config.get("resolution"),
                "pairs": self.filename,
                "skip": self.skip,
                "names": self.pairs.header.columns
            }
        ).df()

    def retrieves_bins(self) -> bool:
        """Return whether or not the objects parsed reliably store the bins"""
        # .pairs files typically store chromsizes as #chromsize: header lines
        return True