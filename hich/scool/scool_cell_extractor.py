from typing import *
import pandas
from abc import ABC, abstractmethod
from hich.scool import CellPixelIter

class ScoolCellExtractor(ABC):
    @property
    def cell_count(self) -> int:
        """Return the current expected number of cells to be yielded from this source"""
        if not hasattr(self, "cell_count"):
            self.cell_count = None
        return self.cell_count
    
    @cell_count.setter
    def cell_count(self, cell_count: int):
        """Set the expected number of cells to be yielded from this source"""
        self.cell_count = cell_count

    @abstractmethod
    @classmethod
    def compatible(cls, source: Any, config: Dict) -> bool:
        """Return whether instances of a ScoolCellExtractor subclass can parse CellPixelIter objects from source using config

        Returns:
            True if cells can and will be parsed by this ScoolCellExtractor from source
        """
        pass

    @abstractmethod
    def claim(self, source: Any, config: Dict) -> None:
        """The ScoolCellExtractor should claim responsibility for parsing this source using the given config

        Arguments:
            source: An object, such as a path string or class instance, that can potentially be used by the ScoolCellExtractor to parse pixels from one or more CellPixelIter objects

            config: A dictionary potentially containing config options to direct how source is parsed 
        """
        pass

    @abstractmethod
    @property
    def common_bins(self) -> Union[None, pandas.DataFrame, Dict[str, pandas.DataFrame]]:
        """Extract and return common bins from the source if possible
        """
        pass

    @abstractmethod
    @common_bins.setter
    def common_bins(self, common_bins: pandas.DataFrame) -> None:
        """Use common_bins for chrom, start, end, and possibly other values"""
        pass

    @property
    @abstractmethod
    def cell_names(self) -> Generator[str, None, None]:
        """Iterate through all cell names found in the source object that can be inserted
        
        Note: Earlier sources have priority on cell names. Depending on ScoolCreator config, conflicts either result in later objects not producing pixels for the cell of that name, or raise an exception. 
        """
        pass

    def cell_bins(self, cell_name: str) -> pandas.DataFrame:
        """Return cell-specific bins.
        
        Arguments:
            cell_name (str): The name of the cell to get cell-specific bins from.

        Returns:
            pandas.DataFrame: Contains per-cell bins values, such as weights. Must be a pandas.DataFrame type or cooler.create_scool raises an exception. If the returned dataframe contains "chrom", "start", and "end", these columns may be replaced with placeholder values by the ScoolCreator as the data is only useful in the first DataFrame when bins are submitted on a per-cell basis.
        """
        pass

    @abstractmethod
    def cell_pixels_iter(self, cell_name: str) -> CellPixelIter:
        """Return CellPixelIter for the given cell name
        
        Arguments:
            cell_name (str): The name of the cell to get cell-specific bins from.

        Returns:
            CellPixelIter: Object capable of communicating with the parent ScoolCellExtractor to signal iteration start and end if necessary and that can yield pandas.DataFrame objects for the per-cell pixels
        """

        pass