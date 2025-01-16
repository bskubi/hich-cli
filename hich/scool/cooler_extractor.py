import pandas
import cooler
from typing import *
from hich.scool import ScoolCellExtractor, CellPixelIter, ScoolCreator

class CoolCellPixelIter(CellPixelIter):
    def __init__(self, parent: "CoolerExtractor", cooler_obj: cooler.Cooler):
        self.parent = parent
        self.cooler = cooler_obj

    def create_scool_iter(self) -> Generator[pandas.DataFrame, None, Any]:
        """Yield pixels as pandas.DataFrame when create_scool iterates through this object

        Returns:
            Generator yielding pandas.DataFrames with at least the columns bin1_id (int), bin2_id (int), count (int)
        """
        for chromname in self.cooler.chromnames:
            yield self.cooler.pixels().fetch(chromname)

class CoolerExtractor(ScoolCellExtractor):
    """Manages extraction from cooler.Cooler objects as well as .cool, .mcool and .scool files
    """

    def __init__(self):
        self._common_bins = None

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

    @classmethod
    def name_coolers_iter(cls, source: Any, config: Dict) -> Generator[Tuple[str, cooler.Cooler], None, None]:
        """Iterate through cell name(s) and cooler.Cooler(s) obtained from source and config

        If source is a cooler.Cooler, yield it; cell name is config['cell_name'].
        If source is a .cool file, build a cooler.Cooler and yield it; cell name is config['cell_name'].
        If source is a .mcool file, look for 'resolution' in config dict and yield that Cooler; cell name is config['cell_name'].
        If source is a .scool file, iterate through all coolers (limited to those in config['cell_names_subset'] and further excluding those in config['cell_names_excluded']); cell names are taken from cooler.fileops.list_scool_cells
        """
        
        if isinstance(source, cooler.Cooler) and config.get("cell_name"):
            # Cooler object
            return config["cell_name"], source
        else:
            # Check for .cool file, then .mcool file
            candidates = [
                source,
                f"{source}::/resolutions/{config.get('resolution')}"
            ]
            for candidate in candidates:
                try:
                    yield config["cell_name"], cooler.Cooler(candidate)
                except:
                    continue
            try:
                # Check for .scool file
                subset = config.get('cell_names_subset') 
                excluded = config.get('cell_names_excluded')
                for cell_name in cooler.fileops.list_scool_cells(source):
                    if subset and cell_name not in subset or excluded and cell_name in excluded:
                        continue 
                    yield cell_name
            except:
                return None
        return None
    
    def cooler(self, cell_name: str) -> Generator[cooler.Cooler, None, None]:
        """Retrieve the Cooler object associated with the cell name"""
        if isinstance(self.source, cooler.Cooler) and self.config.get("cell_name") == cell_name:
            # self.source is a Cooler object
            return self.source
        else:
            # Check for .cool file, then .mcool file, then .scool file
            candidates = [
                self.source,
                f"{self.source}::/resolutions/{self.config.get('resolution')}",
                f"{self.source}::/cells/{cell_name}"
            ]
            for candidate in candidates:
                try:
                    return cooler.Cooler(candidate)
                except:
                    continue
        return None

    @classmethod
    def compatible(cls, source: Any, config: Dict) -> bool:
        """Compatible with cooler.Cooler objects or paths to them  

        Returns:
            True if cells can and will be parsed by this ScoolCellExtractor from source
        """
        return next(CoolerExtractor.name_coolers_iter(source, config)) is not None

    def claim(self, source: Any, config: Dict) -> None:
        """The ScoolCellExtractor should claim responsibility for parsing this source using the given config

        Arguments:
            source: An object, such as a path string or class instance, that can potentially be used by the ScoolCellExtractor to parse pixels from one or more CellPixelIter objects

            config: A dictionary potentially containing config options to direct how source is parsed 
        """
        self.source = source
        self.config = config

    @property
    def common_bins(self) -> Union[None, pandas.DataFrame, Dict[str, pandas.DataFrame]]:
        """Extract and return common bins from the source if possible
        """
        if self._common_bins:
            return self._common_bins
        
        # Retrieve and store common bins if they're not already set
        _, first_cooler = next(iter(self.name_coolers_iter(self.source, self.config)))

        # Concatenate all per-chromname bins
        self._common_bins = pandas.concat([first_cooler.bins().fetch(chromname) for chromname in first_cooler.chromnames])
        return self._common_bins

    @common_bins.setter
    def common_bins(self, common_bins: pandas.DataFrame) -> None:
        """Use common_bins for chrom, start, end, and possibly other values"""
        self._common_bins = common_bins

    @property
    def cell_names(self) -> Generator[str, None, None]:
        """Iterate through all cell names found in the source object that can be inserted
        
        Note: Earlier sources have priority on cell names. Depending on ScoolCreator config, conflicts either result in later objects not producing pixels for the cell of that name, or raise an exception. 
        """
        for cell_name in self.name_coolers_iter(self.source, self.config):
            yield cell_name
        

    def cell_bins(self, cell_name: str) -> pandas.DataFrame:
        """Return bins for the cooler object.
        
        Arguments:
            cell_name (str): The name of the cell to get cell-specific bins from.

        Returns:
            pandas.DataFrame: Contains per-cell bins values, such as weights. Must be a pandas.DataFrame type or cooler.create_scool raises an exception. If the returned dataframe contains "chrom", "start", and "end", these columns may be replaced with placeholder values by the ScoolCreator as the data is only useful in the first DataFrame when bins are submitted on a per-cell basis.
        """
        cell = self.cooler(cell_name)
        if not cell:
            raise ValueError(f"Requested cell name {cell_name} could not be located in the source {self.source} with config {self.config}")
        
        # Concatenate per-chromname bins dicts and return
        return pandas.concat([cell.bins().fetch(chromname) for chromname in cell.chromnames])


    def cell_pixels_iter(self, cell_name: str) -> "CellPixelIter":
        """Return CellPixelIter for the given cell name
        
        Arguments:
            cell_name (str): The name of the cell to get cell-specific bins from.

        Returns:
            CellPixelIter: Object capable of communicating with the parent ScoolCellExtractor to signal iteration start and end if necessary and that can yield pandas.DataFrame objects for the per-cell pixels
        """

        cooler_obj = self.cooler(cell_name)
        if not cooler_obj:
            raise ValueError(f"Requested cell name {cell_name} could not be located in the source {self.source} with config {self.config}")
        
        return CoolCellPixelIter(self, cooler_obj)
    

ScoolCreator.register_default_extractor(CoolerExtractor)