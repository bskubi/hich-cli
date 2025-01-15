import cooler
import pandas
from typing import *
from hich.scool import ScoolInput

class CoolScoolInput(ScoolInput):
    """Interface between a .cool file or Cooler object to cooler.create_scool
    """

    def try_parse(self, obj: Union[cooler.Cooler, str], config: Dict[str, Any]) -> bool:
        """Returns True if obj is a cooler.Cooler or one can be created from it
        
        Arguments:
            obj (Union[cooler.Cooler, str]): The cooler.Cooler or path to a .cool file
            config (Dict[str, Any]): Config dict containing a "cell_name" key with string value that is the cell name
        """
        if "cell_name" not in config:
            return False

        if isinstance(obj, cooler.Cooler):
            self.obj = obj
            self.config = config
            return True

        try:
            self.obj = cooler.Cooler(obj)
            return True
        except:
            return False


    def bins(self, first: bool = True) -> Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]:
        """Generate bins value for cooler.create_scool

        Arguments:
            first (bool): If True, only the first DataFrame in the dictionary (keyed by cell names) will have 'chrom', 'start', and 'end' columns; others will use `None` as placeholders. If False, all DataFrames will use placeholders. This minimizes memory use while meeting `create_scool` requirements for these columns.

        Returns:
            Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]: A single DataFrame with shared bins for all cells, or a dictionary mapping cell names to DataFrames with cell-specific bins (e.g., including 'weights' columns).
        """
        try:
            self.obj: cooler.Cooler
            b = pandas.concat([self.obj.bins().fetch(chromname) for chromname in self.obj.chromnames])
            if not first:
                b["chrom"] = None
                b["start"] = None
                b["end"] = None
            return {self.cell_names: b}
        except:
            raise ValueError(f"{self.obj} parsed as cooler.Cooler by CoolScoolInput, but bins could not be retrieved")

    def cell_name_pixels_dict(self) -> Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]:
        """Generate cell_name_pixels_dict value for cooler.create_scool

        Returns:
            Dict[str, Union[pandas.DataFrame, Iterable[pandas.DataFrame]]]: Maps cell group names to either DataFrames or iterables of DataFrames.
        """
        self.obj: cooler.Cooler

        def iter_pixels():    
            for chromname in self.obj.chromnames:
                yield self.obj.pixels().fetch(chromname)
        
        return {self.cell_names: iter_pixels()}

    def retrieves_bins(self) -> bool:
        """Cooler objects store bins"""
        return True
        