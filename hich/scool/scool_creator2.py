import cooler
from typing import Protocol, Dict, Iterable, Union, List, Any
import pandas
import polars
import duckdb
from pathlib import Path, PurePath
from dataclasses import dataclass, field
import sys
from hich.pairs import PairsHeader, PairsFile
from parse import parse

class BinPixelParser(Protocol):
    """Construct inputs to Open2C cooler create_scool method
    """

    @classmethod
    def can_parse(cls, obj, config: Dict = {}) -> bool:
       """Return whether the IterDictParser can parse the object"""
       ...

    @classmethod
    def cell_names(cls, obj: Any, config: Dict) -> Union[str, List[str]]:
        """Return the name associated with the object"""
        ...

    @classmethod
    def cell_name_bins_dict(
        cls, 
        cell_name: str,
        obj: Any,
        config: Dict,
        shared_bins: Union[None, pandas.DataFrame] = None,
        extended_bins_cols: Union[bool, List[str]] = True,
        *args,
        **kwargs
        ) -> Dict[str, pandas.DataFrame]:
        """Return a {cell_name (str): bins (pandas.DataFrame)} dict
        
        When inserting large batches of cells, some aspects of cooler.create_scool should be kept in mind.

        1. It raises an exception unless bins is a pandas.DataFrame containing the columns "chrom", "start" and "end". However, it does not use these columns unless working on the first item in the entire bins dict.

        1a. In the case where only these three columns are used, the same instance of the bins dataframe can be passed for every cell. This can be created once and specified using shared_bins, in which case typical behavior for a BinPixelParser is to assign that bins dataframe instance as the value for every value in the returned dict.
        
        1b. When passing bins dataframes containing extended columns (i.e. weights), the unnecessary "chrom", "start" and "end" columns must be included, along with the cell-specific bin-associated columns we actually want to write. All of these must be loaded into memory at once for all cells to be written, which could be memory-intensive if the number of cells is large. To mitigate this, typical behavior for the BinPixelParser is to set the value of the "chrom", "start" and "end" columns to None in the bins DataFrame, which should conserve memory consumption when passing in a large number of these DataFrames, but still allow extended columns to be written. Users may wish to filter the extended columns to be written to a subset, in which case they can pass a list of the columns that should be included.
        
        If the shared_bins DataFrame was not given to the ScoolCreator object, then the chrom, start, and end columns of the first bins DataFrame created by BinPixelParser will be used as the shared_bins. It should be sorted by (chrom, start, end).
        """
        ...

    @classmethod
    def cell_name_pixels_dict(
        cls, 
        cell_name: str,
        obj: Union[cooler.Cooler, str],
        config: Dict,
        shared_bins: pandas.DataFrame,
        extended_pixels_cols: Union[bool, List[str]] = True,
        regions_iter: Iterable = None,         
        *args,
        **kwargs
        ) -> Dict[str, Iterable]:
        """Return a {cell_name (str): pixels (Iterable[pandas.DataFrame])} dict
        
        Cooler.create_scool, when receiving a dictionary of this format as its chrom_names_pixels_dict, will iterate through the values for each cell to be written to the .scool file. It expects each yielded object to be a pandas DataFrame containing the columns bin1_id, bin2_id, and count, possibly along with extra extended columns. These columns will be written iteratively for that cell. The bin1_id and bin2_id columns correspond to the 0-indexed offset of a value in the chrom, start, and end datasets and of the corresponding values in the chrom, start, and end row of the shared_bins (when created).

        Arguments:
            cell_name: str: The name of the cell to write the pixels to
            obj: Union[cooler.Cooler, str]: The Cooler object or .cool filename
            config: Dict: Details of how to process the object
            shared_bins: pandas.DataFrame: Shared bins DataFrame, potentially useful to create the pixels DataFrame in some cases
            extended_pixels_cols: Union[bool, List[str]]: If a True boolean, keep all loaded pixels columns. If False, keep only chrom1, start1, end1, chrom2, start2, end2, count. If a list of column names, keep specified subset of extended columns in addition to chrom1, start1, end1, chrom2, start2, end2, count.
            regions_iter (Iterable): Yields pandas.DataFrames covering all pixels to be written for the cell
        """
        ...

class CoolBinPixelParser:
    """Parses cooler.Cooler objects or .cool files"""

    no_cooler_cell_name = ValueError(
"""
Could not determine cell name for Cooler object or file.
Options to associate a Cooler object or file with a cell name include:

1. In the config parameter, pass {"cell": "desired cell name"}
2. In the config parameter, pass {"Cooler.info_cell_key": "key"} where "key" is the key in the Cooler.info metadata dict containing the desired cell name
3. Pass a filename such that Path(filename).name returns a string
"""
)
    @classmethod
    def can_parse(cls, obj, config: Dict = {}) -> bool:
        """Return whether the CoolBinPixelParser can parse the object"""
        if isinstance(obj, cooler.Cooler):
            return True
        elif isinstance(obj, str):
            try:
                cooler.Cooler(obj)
                return True
            except:
                return False
        return False

    @classmethod
    def cell_names(cls, obj: Union[cooler.Cooler, str] = None, config: Dict = {}) -> str:
        """Return the name associated with the object"""
        # Various ways of getting the cell name
        if "cell" in config:
            return config["cell"]
        elif isinstance(obj, str):
            path = Path(obj)
            return str(path.name.removesuffix(path.suffix))
        elif isinstance(obj, cooler.Cooler) and "Cooler.info_cell_key" in config:
            return str(obj.info[config["Cooler.info_cell_key"]])
        else:
            raise CoolBinPixelParser.no_cooler_cell_name

    @classmethod
    def cell_name_bins_dict(
        cls, 
        cell_name: str,
        obj: Union[cooler.Cooler, str],
        config: Dict,
        shared_bins: Union[None, pandas.DataFrame] = None,
        extended_bins_cols: Union[bool, List[str]] = True,
        *args,
        **kwargs
        ) -> Dict[str, pandas.DataFrame]:
        """Return cell bin dict for a cooler.Cooler object or filename to a .cool file
        """
        if not isinstance(cell_name, str):
            raise TypeError(f"cell_name should be str, is {type(cell_name)}")
        # If a filename was passed, create the Cooler object
        if isinstance(obj, str):
            obj = cooler.Cooler(obj)
        # 1. If we received shared bins and we don't want to use extended bins cols, then use the shared bins
        # 2. If we didn't receive shared bins, or if extended bins cols are to be kept, then load bins from the file.
        #   2a. If we received shared bins, but extended bins cols are kept, then set the chrom, start, end to placeholders so we don't pass redundant info to create_scool
        #   2b. If we did not receive shared bins, and extended bin cols are kept, then return the shared cols (chrom, start, end) plus whatever extended bin cols should be kept
        #   2b_1. If we received a list of extended bin col names, use the subset present in the Cooler object
        #   2b_2. If we received a True value for extended bin cols, use the entire loaded bins dataframe

        if shared_bins and isinstance(extended_bins_cols, bool) and not extended_bins_cols:
            # Condition 1
            # Use shared bins if we don't need extended bins columns (i.e. "weight") and
            # shared bins have been passed
            bins = shared_bins
        else:
            # Condition 2
            # We don't have shared bins or we need extended columns
            # Either way we need to load the bins from the cool file.
 
            # Create combined bins dict for all chromosomes, which will be used
            # as-is if shared_bins hasn't been passed and we want to keep the extended columns
            bins = pandas.concat([
                obj.bins().fetch(chromname)
                for chromname in obj.chromnames
            ])

            if shared_bins:
                # Condition 2a
                # cooler.create explicitly validates that the bins is a pandas DataFrame
                # that contains chrom, start, and end columns. However, it doesn't actually use
                # those columns for writing per-cell bin information. In cases where per-cell bins
                # information is being passed, we therefore replace them with a small placeholder
                # to avoid filling memory with a large amount of unnecessary information.
                bins["chrom"] = None
                bins["start"] = None
                bins["end"] = None
            else:
                # Condition 2b
                if isinstance(extended_bins_cols, list):
                    # Condition 2b.1
                    # Get subset of columns present in bins
                    cols = ['chrom', 'start', 'end'] + extended_bins_cols
                    cols = [col for col in cols if col in bins.columns]
                elif isinstance(extended_bins_cols, bool):
                    # Condition 2b.2
                    if extended_bins_cols:
                        cols = bins.columns
                    else:
                        cols = ['chrom', 'start', 'end']
                else:
                    raise ValueError(f"extended_bins_cols should be bool or list of column names, was {type(extended_bins_cols)}")
                bins = bins[cols]
                
                
        # Return the cell_name_bins_dict
        return {cell_name: bins}

    @classmethod
    def cell_name_pixels_dict(
        cls,
        cell_name: str,
        obj: Union[cooler.Cooler, str],
        config: Dict,
        shared_bins: pandas.DataFrame,
        extended_pixels_cols: Union[bool, List[str]] = True,
        regions_iter: Iterable = None,
        *args,
        **kwargs
        ) -> Dict[str, Iterable]:
        """Return cell pixel iter dict for a cooler.Cooler object or filename to a .cool file
        """
        # If received a filename, create Cooler object
        if isinstance(obj, str):
            obj = cooler.Cooler(obj)
        
        # Declare object as unambiguously Cooler
        obj: cooler.Cooler

        CoolBinPixelParser.validate_bin_size(obj, config)

        # Iterate through chromnames by default
        regions_iter = regions_iter or obj.chromnames

        # Create function to yield pandas DataFrames
        def load_regions(c: cooler.Cooler, extended_pixels_cols: List[str], regions_iter: Iterable):
            for region in regions_iter:
                pixels = c.pixels().fetch(region)
                base_cols = ["chrom1", "start1", "end1", "chrom2", "start2", "end2", "count"]
                if isinstance(extended_pixels_cols, bool):
                    if not extended_pixels_cols:
                        pixels = pixels[base_cols]
                elif isinstance(extended_pixels_cols, list):
                    pixels = pixels[base_cols + extended_pixels_cols]
                yield pixels
        
        # Create the pixel iter dict
        return {cell_name: load_regions(obj, extended_pixels_cols, regions_iter)}

    @staticmethod
    def validate_bin_size(c: cooler.Cooler, config):
        # Validate that if a target resolution is specified, the cooler matches
        target_resolution = config.get('resolution', None)
        if target_resolution is not None and c.binsize != target_resolution:
            raise ValueError(f"Cooler.binsize is {c.binsize} but config['resolution'] is {target_resolution}. Set to None for no filter or the true resolution/binsize of this Cooler object.")


class ScoolCreator:
    """Creates inputs and calls cooler.create_scool

    Allows registering parsers for specific objects and file formats
    """
    # Registered parsers for ScoolCreator objects
    parsers: List[BinPixelParser] = None

    @dataclass
    class InputObject:
        """Specifies an input object to parse and details on how to parse it"""
        # Object to parse, which will only be processed if at least one BinPixelParser's .can_parse() method returns True for it
        obj: Any

        # Parser-specific configuration flags
        config: Dict = field(default_factory=dict)

        # Columns other than the base columns 'chrom', 'start', and 'end' to write for the specific cell
        # If True writes all pixels columns, if False writes only the base columns
        extended_bins_cols: Union[bool, List[str], pandas.DataFrame] = True
        
        # Columns other than the base columns 'chrom1', 'start1', 'end1', 'chrom2', 'start2', 'end2', 'count' to write
        # If True writes all pixels columns, if False writes only the base columns
        extended_pixels_cols: Union[bool, List[str], pandas.DataFrame] = True

        # Iterable or generator that returns pixels dataframes in chunks on-the-fly while cooler.create_scool iterates through it
        # Useful to conserve memory by loading pixels in batches from disk as they are being written.
        regions_iter: Iterable[pandas.DataFrame] = None

    def __init__(self):
        self.parsers = ScoolCreator.parsers.copy()


    @classmethod
    def register(cls, parser: BinPixelParser) -> None:
        """Register a parser as the highest-priority parser"""
        if cls.parsers is None:
            cls.parsers = []
        cls.parsers = [parser] + cls.parsers


    def create_scool(self, cool_uri: str, input_objects: List[InputObject], shared_bins: pandas.DataFrame = None, **kwargs):
        """Extracts bins and pixels from specified objects and calls cooler.create_scool to create/append to an .scool file at the given cool_uri

        kwargs are passed to the cooler.create_scool method.

        input_objects: a list of objects to extract from. Currently supported by hich-cli:
            filenames of .cool files (str)
            cooler.Cooler objects (cooler.Cooler)
        

        Additional classes following the BinPixelParser Protocol can be registered with the ScoolCreator class or added to the list of parsers for an ScoolCreator instance to extend compatibility to new objects, file formats, etc.

        Note that if called in append mode (mode = 'a'), then cooler.create_scool will delete and recreate the shared bins group. However, .scool is an HDF5-based data model and in HDF5 and deleting HDF5 groups leaves dead space in the file. If called many times, this could result in a substantially bloated file. This dead space can be removed by installing hdf5-tools and calling h5repack on the file.
        """
        # Will be updated with cell names as keys, bins DataFrames as values
        cell_name_bins_dict = {}

        # Will be updated with cell names as keys, pixel DataFrame iterators/generators or DataFrames
        cell_name_pixels_dict = {}

        for obj in input_objects:
            # Coerce to an input object
            if not isinstance(obj, ScoolCreator.InputObject):
                obj = ScoolCreator.InputObject(obj)
            
            # Get the BinPixelParser capable of parsing this object
            parser: BinPixelParser = self.select_parser(obj.obj)

            if parser:
                # If found, then get the name, bins and pixels and update the bins and pixels dicts
                print(f"Preparing bins and pixels insertion dict for {obj}", file = sys.stderr)
                cell_names = parser.cell_names(obj.obj, obj.config)
                cell_bins = parser.cell_name_bins_dict(cell_names, obj.obj, obj.config, shared_bins, obj.extended_bins_cols)
                shared_bins = shared_bins or cell_bins
                cell_pixels = parser.cell_name_pixels_dict(cell_names, obj.obj, obj.config, shared_bins, obj.extended_pixels_cols, obj.regions_iter)

                cell_name_bins_dict.update(cell_bins)
                cell_name_pixels_dict.update(cell_pixels)
            else:
                print(f"Skipping {obj} as no registered parser was found", file = sys.stderr)

        print(f"All input objects that could be parsed are ready for insertion. Creating {cool_uri}.", file = sys.stderr)
        # Create the .scool file
        cooler.create_scool(
            cool_uri = cool_uri,
            bins = cell_name_bins_dict,
            cell_name_pixels_dict = cell_name_pixels_dict,
            **kwargs
        )
        if Path(cool_uri).exists():
            print(f"{cool_uri} created successfully.", file = sys.stderr)

    def select_parser(self, obj: InputObject) -> Union[None, BinPixelParser]:
        """Select the first (highest-priority) parser that can parse the object"""
        for parser in self.parsers:
            if parser.can_parse(obj):
                return parser
        return None

# Register hich-cli default BinPixelParsers with the ScoolCreator class
ScoolCreator.register(CoolBinPixelParser)
