import click
from hich.fragtag import tag_restriction_fragments

@click.command
@click.option("--batch_size", default = 1000000)
@click.argument("fragfile")
@click.argument("out_pairs")
@click.argument("in_pairs")
def fragtag(batch_size, fragfile, out_pairs, in_pairs):
    tag_restriction_fragments(fragfile, in_pairs, out_pairs, batch_size)