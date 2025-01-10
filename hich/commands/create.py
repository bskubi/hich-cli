import click
from typing import List, Dict
import itertools
from hich.scool import ScoolCreator
from glob import glob
import duckdb as dd
import sys

@click.command
@click.option("-g", "--glob", "globs", default = [], show_default=True, multiple=True, help="File glob of input files to include, can include multiple times")
@click.option("-m", "--mode", "mode", default="w", type = click.Choice(["w", "a"]), show_default=True, help="Overwrite (w) or append (a) to .scool file")
@click.option("-c", "--config", "config_df", default=None, show_default=True, help="Columnar file containing config parameters on a per-input file basis")
@click.option("-b", "--bins", "bins", default=None, show_default=True, type = click.Path(readable=True, dir_okay=False), help="Columnar file containing the headers chrom, start, end; can be created automatically or extracted from some input file types such as .cool/.mcool/.scool")
@click.argument("scool_uri", type = click.Path(writable=True, dir_okay=False))
@click.argument("input_obj", nargs = -1, type = click.Path(writable=True, dir_okay=False))
def create_scool(globs: List[List[str]], mode: str, config_df: str, bins: click.Path, scool_uri: click.Path, input_obj: List[str]) -> None:
    """Create an Open2C format .scool file (a single-resolution, multi-cell file)

    The only currently supported input file type is .cool.
        For .cool files, cell names if not otherwise given are specified from the filename [CELL].cool as the [CELL] prefix
    
    If a columnar config dict is specified (-c, --config), it should have headers. The schema is sniffed by DuckDB. The following headers control insertion as follows:
        filename (required): The input file that config settings for the row will apply to
        cell: The group under which the data for the file will be stored in the .scool file, in [scool_uri]::/cells/[cell]
        resolution: Validates the data is or can be binned at that resolution, and uses it
        extended_bins_cols: A whitespace-separated list of extended bins columns (besides chrom, start, end) to retain for the file
            Note: all bins cols will be used if header not provided; if header provided and cell is blank, no extended cols will be used for the file
        extended_pixels_cols: A whitespace-separated list of extended bins columns (besides chrom, start, end) to retain for the file
            Note: all pixels cols will be used if header not provided; if header provided and cell is blank, no extended cols will be used for the file
    Arguments:
        scool_uri (path): The filepath under which the scool file should be created.
        input_obj (list of paths/glob): The filepaths to insert into the scool file
    """
    print("Getting list of input files", file = sys.stderr)
    # Create a list of input object paths
    input_objects = list(itertools.chain.from_iterable([glob(g) for g in globs])) + list(input_obj)

    # Create ScoolCreator.InputObjects from them that can be configured from the config df
    input_objects = {obj: ScoolCreator.InputObject(obj) for obj in input_objects}


    # Configure the created InputObjects
    if config_df:
        print(f"Parsing configuration file {config_df}", file = sys.stderr)
        config_df = dd.read_csv(config_df).pl()
        for row in config_df.iter_rows(named=True):
            row: Dict[str, Any]
            filename = row.get("filename")
            if filename not in input_objects:
                input_objects[filename] = ScoolCreator.InputObject(filename)
            if not filename:
                break
            extended_bins_cols = row.get("extended_bins_cols", None)
            input_objects[filename].extended_bins_cols = extended_bins_cols.split() if extended_bins_cols else []

            extended_pixels_cols = row.get("extended_bins_cols", None)
            input_objects[filename].extended_bins_cols = extended_pixels_cols.split() if extended_pixels_cols else []
            if "resolution" in row:
                input_objects[filename].config.update({"resolution": row["resolution"]})
            if "cell" in row:
                input_objects[filename].config.update({"cell": row["cell"]})

    # Convert back to a list of InputObjects
    input_objects = list(input_objects.values())


    

    # Load shared bins
    print(f"Loading bins from {bins}", file = sys.stderr)
    shared_bins = dd.read_csv(bins) if bins else None
    scool_creator = ScoolCreator()

    print(f"Preparing to insert {len(input_objects)} objects", file = sys.stderr)
    scool_creator.create_scool(
        scool_uri,
        input_objects,
        shared_bins,
        mode=mode
    )