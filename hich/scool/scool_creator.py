from hich.scool import ScoolCellExtractor, CellPixelIter
from typing import *
import cooler
import pandas

class ScoolCreator:
    """Coordinates ScoolCellExtractors to build bins and cell_name-pixels_dict args to cooler.create_scool

    Intended use is to call offer() method with a source and config object compatible with one of the ScoolCellExtractors for each source to be inserted into the scool file. After all sources have been offered, then call bins_pixels() to build the unified arguments across all sources. Finally, call cooler.create_scool using these arguments.

    Example:

    scool_creator = ScoolCreator()    
    scool_creator.offer("cell1.cool", {"cell_name": "cell1"})
    scool_creator.offer("cell2.cool", {"cell_name": "cell2"})
    bins, pixels = scool_creator.bins_pixels()
    cooler.create_scool("output.scool", bins = bins, cell_name_pixels_dict = pixels)
    """
    default_extractors: List[Type[ScoolCellExtractor]] = []

    def __init__(self):
        self.extractor_types: List[Type[ScoolCellExtractor]] = ScoolCreator.default_extractors.copy()
        self.extractors: List[ScoolCellExtractor] = []

    @classmethod
    def register_default_extractor(cls, extractor: Type[ScoolCellExtractor]):
        """Register highest-priority ScoolCellExtractor to try for parsing objects"""
        cls.default_extractors = [extractor] + cls.default_extractors

    def offer(self, source: Any, config: Dict) -> Optional[int]:
        """Use the first compatible ScoolCellExtractor to claim the source for extraction
        
        Arguments:
            source (Any): An object some ScoolCellExtractor may be able to use to yield cell pixels
            config (Dict): Configuration objects for source

        Returns:
            The number of cells that will be extracted if a compatible ScoolCellExtractor was found, or None if no compatible ScoolCellExtractor was found
        """
        # Iterate through registered ScoolCellExtractor types in order of priority, and have the first compatible one claim the source and config for later extraction 
        for extractor_type in self.extractor_types:
            if extractor_type.compatible(source, config):
                extractor = extractor_type()
                extractor.claim(source, config)
                self.extractors.append(extractor)

    def bins(self, common_bins: Optional[pandas.DataFrame] = None) -> Union[pandas.DataFrame, Dict[str, pandas.DataFrame]]:
        """Extract common and per-cell bins from all sources
        
        Arguments:
            common_bins: Optional[pandas.DataFrame] = None -- Pregenerated common bins for all cells in the scool file. Optional if it can be generated from the source.

        """
        bins_dict = {}
        for extractor in self.extractors:
            if common_bins:
                extractor.common_bins = common_bins

            # Get common bins if needed
            common_bins = common_bins or extractor.common_bins

            if not common_bins:
                raise ValueError("{str(extractor)} could not generate common bins, and none were supplied. Either submit pregenerated common bins to ScoolCreator.bins_dict or ensure that the first submitted source object and its claiming ScoolCellExtractor can create the required bins dataframe.")

            for cell_name in extractor.cell_names:
                # Earlier sources have priority over cell names, so ignore this cell name if it's already stored
                if cell_name in bins_dict:
                    extractor.cell_count = extractor.cell_count - 1
                    continue
                
                bins = extractor.cell_bins(cell_name)

                if bins_dict:
                    # If we already have at least one bins dataframe in the bins_dict, then further entries either should be references to common_bins (if there are no columns other than "chrom", "start", "end") or should replace "chrom", "start" and "end" with None placeholders since these will just be ignored and dropped by create_scool and take up memory otherwise.
                    if (
                        set(bins.columns) == {"chrom", "start", "end"}
                        and bins[["chrom", "start", "end"]].equals(common_bins[["chrom", "start", "end"]])
                        ):
                        # Replace bins with common bins to minimize memory usage
                        bins = common_bins
                    else:
                        # If there are cell-specific bins values (i.e. weights), replace common columns with placeholders as their absence raises an exception in cooler.create_scool but they aren't actually used for anything after the first bins in the dict
                        bins["chrom"] = None
                        bins["start"] = None
                        bins["end"] = None
                        
                        if len(bins) != len(common_bins):
                            # Ensure either the bin counts match when cell specific bins are present
                            cols = [col for col in bins.columns if col not in ["chrom", "start", "end"]]
                            
                            raise ValueError(f"For {str(extractor)} and cell_name {cell_name}, cell-specific bin columns were returned, but the row count did not match the row count for the common bins dataframe. All cells must have identical numbers of rows in their bins dataframes.\nCommon bins:\n{common_bins}\nCell-specific bins:\n{bins[[cols]]}")
                bins_dict[cell_name] = bins
    
        return bins_dict if bins_dict else common_bins
    
    def cell_name_pixels_dict(self) -> Dict[str, Union[Iterable, pandas.DataFrame]]:
        """Create per-cell pixels iterators from all sources
        
        Arguments:
            common_bins: Optional[pandas.DataFrame] = None -- Pregenerated common bins for all cells in the scool file. Optional if it can be generated from the source.
        """
        cell_name_pixels_dict = {}
        for extractor in self.extractors:
            for cell_name in extractor.cell_names:
                # Earlier sources have priority over cell names, so ignore this cell name if it's already stored
                if cell_name in cell_name_pixels_dict:
                    extractor.cell_count = extractor.cell_count - 1
                    continue
                
                # Stores a CellPixelIter that yields one or more pandas.DataFrames for the cell's pixels
                cell_name_pixels_dict[cell_name] = extractor.cell_pixels_iter(cell_name)
        
        return cell_name_pixels_dict
    
    def bins_pixels(
            self, 
            common_bins: pandas.DataFrame
            ) -> Tuple[
                Union[pandas.DataFrame, Dict[str, pandas.DataFrame]], 
                Dict[str, Union[Iterable, pandas.DataFrame]]
                ]:
        """Returns values for bins and cell_name_pixels_dict ready to pass to cooler.create_scool
        
        Use like:

        scool_creator: ScoolCreator

        bins, cell_name_pixels_dict = scool_creator.bins_pixels()

        cooler.create_scool('output.scool', bins = bins, cell_name_pixels_dict = cell_name_pixels_dict)

        Arguments:
            common_bins: Optional[pandas.DataFrame] = None -- Pregenerated common bins for all cells in the scool file. Optional if it can be generated from the source.

        Returns: Tuple[
                Union[pandas.DataFrame, Dict[str, pandas.DataFrame]], 
                Dict[str, Union[Iterable, pandas.DataFrame]]
                ]

            -- First element of returned tuple is bins, second element is cell_name_pixels_dict
        """
        # Bins should be generated before pixels as some ScoolCellExtractors will require bins in order to produce pixels, so this function's purpose is to enforce that order of creation
        bins = self.bins(common_bins)
        cell_name_pixels_dict = self.cell_name_pixels_dict()
        return bins, cell_name_pixels_dict