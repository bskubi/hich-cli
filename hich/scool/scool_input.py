from typing import *
import pandas
from abc import ABC, abstractmethod

def ScoolInput(ABC):
    """Interface between a specific input filetype to cooler.create_scool
    """

    @abstractmethod
    def try_parse(self, obj: Any, cell_names: Union[str, List[str]]) -> bool:
        """If returns True, the ScoolInput stores and can parse obj
        
        Arguments:
            obj (Any): The object to examine for the ability to parse it
            cell_names (Union[str, List[str]]): A cell name or list of cell names to store and use
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def cell_name_pixels_dict(self) -> Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]:
        """Generate cell_name_pixels_dict value for cooler.create_scool

        Returns:
            Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]: Maps cell group names to either DataFrames or iterables of DataFrames.
        """
        pass

    @abstractmethod
    def retrieves_bins(self) -> bool:
        """Return whether or not the objects parsed reliably store the bins"""
        pass