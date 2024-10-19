import click
from hich.cli import *

@click.group
def hich():
    pass

@hich.group 
def view(): pass

hich.add_command(compartments)
hich.add_command(downsample)
hich.add_command(digest)
hich.add_command(fragtag)
hich.add_command(organize)
hich.add_command(hicrep)
hich.add_command(reshape)
hich.add_command(stats)
hich.add_command(stats_aggregate)
view.add_command(hicrep_comparisons)

if __name__ == "__main__":
    hich()
