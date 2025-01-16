from hich.scool import ScoolInput
from typing import *
import cooler
import pandas

class ScoolCreator:
    default_scool_input: List[Type[ScoolInput]] = []

    def __init__(self):
        self.scool_input = [scool_input() for scool_input in ScoolCreator.default_scool_input]

    @classmethod
    def register(self, scool_input: ScoolInput):
        """Register a default ScoolInput parser in the highest-priority position"""
        ScoolCreator.default_scool = [scool_input] + ScoolCreator.default_scool_input
    
    def create_scool(
            self,
            scool_sources: List[Tuple[Any, Union[str, List[str]]]],
            cool_uri: str):
        """Create scool file at chosen uri
        
        Arguments:
            scool_sources (List[Tuple[Any, Union[str, List[str]]]]): List of tuples of source objects and associated cell names that may be parsed by an ScoolInput object
            cool_uri (str): path to output scool file
        """
        # Accumulate main data sources used to build scool file
        common_bins_df: pandas.DataFrame = None
        bins: Dict[str, pandas.DataFrame] = {}
        cell_name_pixels_dict: Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]] = {}

        # Iterate through scool sources, which may be diverse object or file types 
        for obj, cell_names in scool_sources:
            # Iterate through potential ScoolInput parsers of sources
            for scool_input in self.scool_input:
                # ScoolInput parsers are in order of priority, so select the first that can parse it
                if scool_input.try_parse(obj, cell_names):
                    # Update the bins and pixels dicts
                    new_bins = scool_input.bins(first = not common_bins_df)

                    
                    if isinstance(new_bins, dict):
                        # If it returns a cell_name_bins_dict
                        bins.update(new_bins)
                    elif isinstance(new_bins, pandas.DataFrame):
                        # If it returns a bare pandas DataFrame
                        if not common_bins_df:
                            common_bins_df = new_bins[["chrom", "start", "end"]]

                    cell_name_pixels_dict.update(scool_input.cell_name_pixels_dict())

        # Create the scool file
        cooler.create_scool(
            cool_uri,
            bins,
            cell_name_pixels_dict
        )
