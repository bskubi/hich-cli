# %% 

import cooler
import pandas as pd
from functools import singledispatch
from warnings import warn
import duckdb as dd
import smart_open_with_pbgzip
from smart_open import smart_open
import polars as pl
from typing import Protocol, Dict, Iterable, Union

# Load cooler object
c = cooler.Cooler('/home/benjamin/Documents/tempdata/test.cool')
scool = '/home/benjamin/Documents/tempdata/test.scool'

# Concatenate all bins to a single Pandas dataframe
def all_bins(c: cooler.Cooler) -> pd.DataFrame:
    return pd.concat([
        c.bins().fetch(chromname)
        for chromname
        in c.chromnames
    ])

##############################################################################
# Chrom pixel iterators
# Yield per-chromosome pandas.DataFrame of pixels for files/objects
# containing a *single* bulk sample/cell
##############################################################################

##############################################################################
# Base chrom pixel iterator
##############################################################################
@singledispatch
def chrom_pixel_iter(pixel_source, resolution: int = None):
    """Yield pixel_source

    Args:
        pixel_source: Should be a data structure that cooler's create_scool method can read from
        resolution (int): Target resolution, ignored for this function

    Yields:
        pixel_source
    """
    yield pixel_source

##############################################################################
# Cooler chrom pixel iterator
##############################################################################
@chrom_pixel_iter.register
def _(c: cooler.Cooler, resolution: int = None):
    """Iterate through pixels Pandas DataFrames

    Args:
        c (cooler.Cooler): The cooler API Cooler object
        resolution (int, optional): Optionally, validate cooler object is correct resolution. Defaults to None.

    Raises:
        ValueError: If resolution of Cooler object does not match integer 'resolution' parameter passed

    Yields:
        _type_: Pandas DataFrame with pixel data
    """
    if resolution is not None and resolution != c.binsize:
        # Ensure the cooler file is of the target resolution, if specified
        raise ValueError(f"Specified resolution {resolution}, but cooler object has resolution {c.binsize}")
    for chromname in c.chromnames:
        # Iterate through chromosome names and yield as a Pandas DataFrame
        yield c.pixels().fetch(chromname)

##############################################################################
# Mcool chrom pixel iterator
##############################################################################
def mcool_chrom_pixel_iter(filepath: str, resolution: int = None):
    """Generate pixel iter at appropriate resolution from .mcool file

    Args:
        filepath (str): Path to a .mcool file
        resolution (int, optional): Target resolution. Defaults to None.

    Raises:
        ValueError: If no resolution passed to a .mcool file containing 2+ resolutions
        ValueError: If the specified resolution is not found

    Returns:
        Generator: Generates Pandas DataFrames for the per-chrom pixels for the specified resolution
    """
    # Get the resolutions and groups from all the cooler objects in the .mcool file
    coolers = cooler.fileops.list_coolers(filepath)

    # Get all the resolutions from the multires cooler object
    resolutions = [cooler.Cooler(f"filepath::{c}").binsize for c in coolers]

    if resolution is None:
        # Raise an error if there are multiple resolutions in the .mcool but no resolution specified
        if len(resolutions) > 1:
            raise ValueError(f"More than one resolution found in {filepath}: {coolers}. However, no specific resolution was specified. Scool format expects one resolution.")
        else:
            # If no resolution specified but only one present, use that.
            resolution = resolutions[0]

    # Get the index of the resolution specified, which corresponds to the index of the matching cooler object
    cooler_idx = resolutions.find(resolution)

    # If the resolution the user submitted is not present, raise an error
    # letting them know which ones are available.
    NOT_FOUND = -1
    if cooler_idx == NOT_FOUND:
        raise ValueError(f"Resolution {resolution} not found in {filepath}. Coolers include {coolers}.")
    else:
        # If the resolution was found, then select the corresponding cooler object group path
        cooler_group = coolers[cooler_idx]
        
        # Get the cooler object for the chosen resolution
        c = cooler.Cooler(f"filepath::{cooler_group}")

        # Return an iterator for its pixels
        return chrom_pixel_iter(c, resolution)

# def pairs_chrom_pixel_iter(filepath: str, bins: "DataFrame", resolution: int = 1):
#     """Iterate through per-chromosome pixels from a bulk .pairs file at given uniform resolution

#     Args:
#         filepath (str): .pairs-format file
#         bins (pd.DataFrame): Dataframe containing at least id, chrom, and start positions
#         resolution (int, optional): Binning resolution. Defaults to None.

#     Raises:
#         NotImplementedError: _description_
#         NotImplementedError: _description_
#         ValueError: _description_

#     Yields:
#         pandas.DataFrame: Per-chromosome dataframes with pixel data
#     """
    
#     for line in smart_open(filepath, "rt"):
#         # Iterate through .pairs file header to find #columns: row
#         if line.startswith("#columns:"):
#             # All elements of the split line beside the first ("#columns:") are column names
#             pairs_columns = line.split()[1:]
    
#     # Format a SQL list based on the list of column names
#     columns_sql = "LIST_VALUE(" + ", ".join(f"'{col}'" for col in pairs_columns) + ")"

#     # Create an in-memory database (which will stream to disk for out-of-core processing if necessary)
#     # from the pairs file
#     with dd.connect(":memory:") as conn:
#         # Ingest the necessary columns from pairs
#         conn.execute(f"CREATE TABLE pairs AS SELECT chrom1, pos1, chrom2, pos2 FROM read_csv('{filepath}', names={columns_sql});")

#         # Warnings about inadequate validation
#         warn("Ability to retrieve all distinct chroms from multi-chrom .pairs file not tested yet")
#         warn("Assumes chrom columns are named chrom1 and chrom2")

#         # Bin all pairs at the specified resolution and store in another table
#         sql = (
# """
# CREATE TABLE binned_pairs AS 
# SELECT
#     chrom1,
#     CAST(FLOOR(pos1/$res) AS INTEGER)*$res AS start1,
#     chrom2,
#     CAST(FLOOR(pos2/$res) AS INTEGER)*$res AS start2,
#     COUNT(*) AS count
# FROM pairs
# GROUP BY 
#     chrom1,
#     start1,
#     chrom2,
#     start2;

# -- We no longer need the bp-resolution pairs table.
# DROP TABLE pairs;
# """
# )
#         conn.execute(sql, {"res": resolution})

#         # Get all unique chrom1 chromosomes
#         chromnames = conn.execute("SELECT DISTINCT chrom1 AS chrom FROM pairs").pl()["chrom"]

#         # Warn about inadequate input validation
#         warn("Assumes bins has an index or index column")

#         for chromname in chromnames:
#             # Iterate through all chromname and yield pixels columns as Pandas DataFrames
#             sql = (
# """
# SELECT bins1.bin_id AS bin1_id, bins2.bin_id AS bin2_id, count
# FROM binned_pairs
# JOIN bins AS bins1
# ON
#     binned_pairs.chrom1 = bins1.chrom
#     AND binned_pairs.start1 = bins1.start
# JOIN bins AS bins2
# ON
#     binned_pairs.chrom2 = bins2.chrom
#     AND binned_pairs.start2 = bins2.start
# WHERE binned_pairs.chrom1 = $chromname
# ORDER BY bin1_id, bin2_id
# """
#             )
#             yield conn.execute(sql, {"chromname": chromname}).df()

##############################################################################
# Filename chrom pixel iterator
##############################################################################
@chrom_pixel_iter.register
def _(filepath: str, resolution: int = None):
    """Return per-chromosome pixel DataFrame iterators from filename string

    Args:
        filepath (str): Path to a .mcool or .pairs file (.pairs not implemented yet)
        resolution (int, optional): Target resolution. Defaults to None.

    Raises:
        NotImplementedError: .pairs cannot yet be parsed
    """
    if filepath.endswith(".mcool"):
        # Get the pixel iterator for the cooler of the appropriate resolution
        # within the .mcool file
        mcool_chrom_pixel_iter(filepath, resolution)

    elif filepath.endswith(".pairs"):
        # If called here, assumes the entire .pairs file is for a single
        # bulk sample or cell
        raise NotImplementedError("chrom_pixel_iter not implemented yet for .pairs files")

##############################################################################
# Pixels dict methods
# Returns a {cell (str): pixels (pandas.DataFrame)} dict for the object.
# Should have a way of parsing the object and selecting a subset of objects
# it contains.
##############################################################################

def pairs_pixels_dict(filepath: str, resolution: int):
    # Called when a .pairs file contains interleaved cells

    raise NotImplementedError("pairs_pixels_dict not implemented yet for .pairs files")


def scool_pixels_dict(filepath: str, resolution: int):
    # Get the resolutions and groups from all the cooler objects in the .mcool file
    coolers = cooler.fileops.list_coolers(filepath)

    pixels_dict = {}

    # Accumulate {cell: pixels_iter} objects in the pixels_dict
    # Scool files are single-resolution. In principle one check should be enough
    # but we will just check all the coolers it contains.
    # Validate that all coolers in the .scool file match the target resolution
    for c in coolers:
        # Get group of each cooler
        group = f'{filepath}::{c}'

        # Determine resolution/binsize
        cooler_resolution = c.Cooler(group).binsize

        # Raise an error if it doesn't match the target
        if cooler_resolution != resolution:
            raise ValueError(f'For {group}, resolution is {cooler_resolution} but target resolution is {resolution}.')

        # Parse the cell name as the terminal group
        cell = c.split('/')[-1]

        # Create a pixels_iter for the cell
        pixels_iter = pixels_iter(cooler.Cooler(group))

        # Add the cell name and pixels iter to the pixels dict
        pixels_dict.update({cell: pixels_iter})
    
    return pixels_dict



# TODO: Create methods to update cell_name_pixels_dict for each supported file type
# TODO: Create methods to produce the bins DF for each supported file type
# TODO: Construct the bins and cell_name_pixels_dict dictionaries
#           We can do this by having a boolean dictating whether the file should include chrom, start, end in the bins df for each cell
#           If there are no other columns it returns None and this gets substituted by the chrom, start, end df
# TODO: Create CLI interface
# TODO: Test the tool to build scool files

bins = all_bins(c)
bins["bin_id"] = bins.index


class IterDictParser(Protocol):
    """Construct inputs to Open2C cooler create_scool method
    """

    def bin_iter_dict(
            self, 
            object,
            config
            ) -> Dict[str, Union[Iterable, pandas.DataFrame]]: ...
    """Return bin iter dict for a specific type of object

    Args:
        object: The specific object type supported by class instantiating this protocol
        config: An object controlling how the object is inspected

    Returns:
        bin iter dict

    A bin iter dict is of the format: {scool_cell (str): iter (Iterator)} where iter yields pandas. DataFrames with per-cell bin info for chunked insertion into an scool created via the cooler.create_scool method.

    Note that the first bins DataFrame will have its chrom, start, end columns used for all cells. The remainder will have these columns ignored, while the other cell-specific columns (like weight) will be saved under the cell group.

    Typically, iter should be a generator method that loads pandas.DataFrames on the fly as they are ready to be inserted.
    """

    def pixel_iter_dict(
            self, 
            object,
            config
            ) -> Dict[str, Iterable]: ...
    """Return pixel iter dict for a specific type of object

    Args:
        object: The specific object type supported by class instantiating this protocol
        config: An object controlling how the object is inspected

    Returns:
        pixel iter dict

    A pixel iter dict is of the format: {scool_cell (str): iter (Iterator)}
    Where iter yields pandas.DataFrames for chunked insertion into an scool
    created via the cooler.create_scool method. Typically, iter should be a
    generator method that loads pandas.DataFrames on the fly as they are ready
    to be inserted.
    """

# cooler.create_scool(
#     cool_uri = scool,
#     bins = bins,
#     cell_name_pixels_dict = {"test": pixel_iter(c)},
#     ordered = True,
#     mode = 'a',
# )

# %%
