import click
from hich.cli import IntList, StrList, BooleanList
from hich.hicrep_combos import hicrep_combos

@click.command
@click.option("--resolutions", type = IntList, default = 10000)
@click.option("--chroms", "--include_chroms", type = StrList, default = None)
@click.option("--exclude", "--exclude_chroms", type = StrList, default = None)
@click.option("--chrom-filter", type=str, default = "chrom if size > 5000000 else None")
@click.option("--h", type = IntList, default = "1")
@click.option("--d-bp-max", type = IntList, default = "-1")
@click.option("--b-downsample", type = BooleanList, default = False)
@click.option("--nproc", type=int, default=None)
@click.option("--output", type=str, default = None)
@click.argument("paths", type=str, nargs = -1)
def hicrep(resolutions, chroms, exclude, chrom_filter, h, d_bp_max, b_downsample, nproc, output, paths):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    result = hicrep_combos(resolutions, chroms, exclude, chrom_filter, h, d_bp_max, b_downsample, nproc, output, paths)
    if result is not None:
        click.echo(result)