import duckdb as ddb
import cooler
from typing import *
import duckdb
import polars
import pandas

def aggregate_pairs(pairs, db_name, res, names, skip, bins, barcode_colname = 'readID', barcode_regex = '^[^:]+', barcode_group = 0):
    """Count pairs at given resolution at single-cell resolution
    """
    names = '[' + ', '.join(f"'{name}'" for name in names) + ']'
    with ddb.connect(db_name) as conn:
        result = conn.execute(
            f"""
DROP TABLE IF EXISTS counts2d;
DROP TABLE IF EXISTS barcodes;

CREATE SEQUENCE barcode_id_seq;

CREATE TABLE barcodes (
    id INTEGER DEFAULT nextval('barcode_id_seq'),
    barcode VARCHAR
);

INSERT INTO barcodes (barcode)
SELECT DISTINCT
    regexp_extract({barcode_colname}, '{barcode_regex}', {barcode_group}) AS barcode,
FROM
    read_csv('{pairs}', skip={skip}, names={names});

-- Count pairs at given binning resolution
-- Subtract 1 from pos1 and pos2 to match cooler cload pairs output
CREATE TABLE counts2d AS

WITH pixels_cte AS (
    SELECT
        regexp_extract({barcode_colname}, '{barcode_regex}', {barcode_group}) AS barcode,
        chrom1,
        CAST(FLOOR((pos1-1)/{res}) AS INTEGER)*{res} AS start1,
        chrom2,
        CAST(FLOOR((pos2-1)/{res}) AS INTEGER)*{res} AS start2,
        COUNT(*) AS count
    FROM read_csv('{pairs}', skip={skip}, names={names})
    GROUP BY barcode, chrom1, chrom2, start1, start2
)
SELECT barcodes.id AS barcode_id, chrom1, start1, chrom2, start2, count
FROM pixels_cte
JOIN barcodes
ON pixels_cte.barcode = barcodes.barcode
        """)


class ScoolInput(Protocol):
    def cells(cls):
        pass
    
    def bins(cls):
        pass

    def cell_name_pixels_dict(cls):
        pass

class PairsScoolInput:

    @classmethod
    def parse_header(cls, pairs_filename: str) -> Tuple[List[str], int]:
        """Extract column names and line count from header
        
        Returns:
            Tuple[List[str], int]: A list of column names and the line count of the header
        """
        # Header lines start with '#'.
        # Column names are on the last line, starting with '#columns:'
        # followed by whitespace-delimited column names.
        skip = 0
        with open(pairs_filename) as f:
            for line in f:
                # Count up the number of lines in the header
                skip += 1
                if line.startswith("#columns:"):
                    columns_row = line
                    break

        # Discard '#columns:' and use the rest of the whitespace-delimited entries on the columns line
        # as the column names 
        names = columns_row.split()[1:]
        return names, skip

    def aggregate(
            self, 
            pairs_filename: str,
            bins: "DataFrame",
            resolution: int = 1, 
            db: str = ":memory:",
            cell_colname = 'readID', 
            cell_regex = '^[^:]+', 
            cell_group = 0
            ) -> None:
        
        # Add explicit index column to bins DataFrame as we will use it as the bin1_id and bin2_id values 
        if "index" not in bins.columns:
            bins['index'] = bins.index

        # Create a connection, potentially to a temporary in-memory database
        self.conn = duckdb.connect(db)

        # Extract column names from pairs file
        names, skip = PairsScoolInput.parse_header(pairs_filename)

        # names is a list of column names in the .pairs file, but needs to be formatted
        # into a string representation that SQL can use.
        # names = '[' + ', '.join(f"'{name}'" for name in names) + ']'

        # Aggregate the pairs file to a pixels table at the given resolution
        # Use a regex to parse the cell barcodes and retain them so that
        # the cell name can be associated with its observations
        self.conn.execute(
            f"""
-- The aggregated bin pair counts will be inserted into a fresh pairs table

DROP TABLE IF EXISTS pixels;

-- Count pairs at given binning resolution for each cell
-- Subtract 1 from pos1 and pos2 to match cooler cload pairs output

CREATE TABLE pixels AS

WITH pixels_cte AS (
    
    -- Coarsen bp-resolution observations to the given resolution, then count the number
    -- of pairs for that cell mapping to that position. The user can use regexp_extract to
    -- convert a column of their choice to the cell name.

    SELECT
        regexp_extract({cell_colname}, $cell_regex, $cell_group) AS cell,
        chrom1,
        CAST(FLOOR((pos1-1)/$resolution) AS INTEGER)*$resolution AS start1,
        chrom2,
        CAST(FLOOR((pos2-1)/$resolution) AS INTEGER)*$resolution AS start2,
        COUNT(*) AS count
    FROM read_csv($pairs_filename, skip=$skip, names=$names)
    GROUP BY cell, chrom1, chrom2, start1, start2
)

-- scool files normalize to two tables: bins and pixels
-- bin is (chrom, start, end)
-- pixels are (bin1_id, bin2_id, count), possibly with additional columns
-- We take the premade bins dataframe and join it to the pixels to accomplish this normalization

SELECT cell, bins1.index AS bin1_id, bins2.index AS bin2_id, count
FROM pixels_cte
JOIN bins AS bins1
ON
    pixels_cte.chrom1 = bins1.chrom
    AND pixels_cte.start1 = bins1.start
JOIN bins AS bins2
ON
    pixels_cte.chrom2 = bins2.chrom
    AND pixels_cte.start2 = bins2.start
        """,
        {
            "cell_regex": cell_regex,
            "cell_group": cell_group,
            "resolution": resolution,
            "pairs_filename": pairs_filename,
            "skip": skip,
            "names": names

        })

    def cells(self) -> duckdb.DuckDBPyConnection:
        """Retrieve unique cell names from the pixels table"""
        return self.conn.execute("SELECT DISTINCT cell FROM pixels")

    def cell_name_pixels_dict(self) -> Dict[str, Iterable]:
        """Return a dict mapping cell names to iterables returning pandas DataFrames of the cell's pixels"""
        self.yielded = 0
        def pixels_dict(cell: str) -> Iterable[pandas.DataFrame]:
            """Helper method to extract pixels dict for cell from pixels table"""
            self.yielded += 1
            print("Yielded", self.yielded)
            yield self.conn.execute(
                """
SELECT bin1_id, bin2_id, count
FROM pixels
WHERE pixels.cell = $cell
ORDER BY bin1_id, bin2_id
                """,
                {"cell": cell}
            ).df()
        
        return {
            cell: pixels_dict(cell)
            for cell
            in self.cells().pl()['cell']
        }
        

pairs_filename = "/home/benjamin/Documents/sciMET_GCC/T1.pairs"
persist = "T1.omicdb"
resolution = 20000

print("Retrieving bins")
c = cooler.Cooler("/home/benjamin/Documents/sciMET_GCC/T1.cool")
bins = pandas.concat([c.bins().fetch(chromname) for chromname in c.chromnames])

# print("Creating PairsScoolInput")
# psi = PairsScoolInput()

# print("Aggregating pairs")
# psi.aggregate(pairs_filename, bins, 20_000, db = persist)

# print("Creating scool")
# cooler.create_scool("test.scool", bins, psi.cell_name_pixels_dict())
import time

#print("Aggregating pairs")
#aggregate_pairs(pairs_filename, persist, resolution, *PairsScoolInput.parse_header(pairs_filename), bins)

# print("Partitioning")

# with duckdb.connect(persist) as conn:
#     conn.execute(
#         """
# SET preserve_insertion_order = false;

# SET threads=2;

# COPY counts2d TO 'counts2d_parquet'
# (FORMAT PARQUET, PARTITION_BY(barcode_id))
# ;
# """
#     )


print("Perftesting")
c = 0
o = 0
count = 0
for cell in cooler.fileops.list_coolers("test.scool"):
    s = time.time()
    cooler.Cooler(f"test.scool::{cell}").pixels(join=True).fetch("chr1")
    c += time.time() - s

    cellname = cell.split('/')[-1]
    s = time.time()
    with duckdb.connect(persist) as conn:
        conn.execute(
"""
SELECT * 
FROM read_parquet('counts2d_parquet/barcode_id=1/*.parquet')
WHERE chrom1 = 'chr1'
"""
        ).df()
    o += time.time() - s
    count += 1
    print(count)

    if count == 10:
        break
print("Cooler runtime:", c, "OmicDB runtime:", o)
    