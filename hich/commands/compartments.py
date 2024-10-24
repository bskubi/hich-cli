import click
from pathlib import Path
from hich.cli import StrList
from hich.compartments import write_compartment_scores

@click.command
@click.option("--chroms", type = StrList, default = None)
@click.option("--exclude-chroms", type = StrList, default = None)
@click.option("--keep-chroms-when", type = str, default = None)
@click.option("--n_eigs", type = int, default = 1)
@click.argument("reference")
@click.argument("matrix")
@click.argument("resolution", type = int)
def compartments(chroms, exclude_chroms, keep_chroms_when, n_eigs, reference, matrix, resolution):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    
    matrix = Path(matrix)
    reference = Path(reference)
    final_suffix = matrix.suffixes[-1]
    prefix = matrix.name.rstrip(final_suffix)

    write_compartment_scores(prefix, matrix, reference, resolution, chroms, exclude_chroms, keep_chroms_when, n_eigs)