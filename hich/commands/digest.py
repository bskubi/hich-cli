import click
from hich.digest import make_fragment_index

@click.command()
@click.option("--output", default = None, show_default = True, help = "Output file. Compression autodetected by file extension. If None, prints to stdout.")
@click.option("--startshift", default = 0, show_default = True, help = "Fixed distance to shift start of each fragment")
@click.option("--endshift", default = 0, show_default = True, help = "Fixed distance to shift end of each fragment")
@click.option("--cutshift", default = 1, show_default = True, help = "Fixed distance to shift cutsites")
@click.argument("reference")
@click.argument("digest", nargs = -1)
def digest(output, startshift, endshift, cutshift, reference, digest):
    """
    In silico digestion of a FASTA format reference genome into a
    BED format fragment index.

    Allows more than 800 restriction enzymes (all that are supported by
    Biopython's Restriction module, which draws on REBASE).

    Digest can also be specified as a kit name. Currently supported kits:

        "Arima Genome-Wide HiC+" or "Arima" -> DpnII, HinfI
    
    Multiple kits and a mix of kits and enzymes can be added. Duplicate
    kits, enzyme names, and restriction fragments are dropped.

    Can read compressed inputs using decompression autodetected and supported
    by the Python smart_open library. Can compress output using compression
    formats autodetected by the Polars write_csv function.

    Format is:
    chrom start cut_1
    chrom cut_1 cut_2

    The startshift param is added to all values in column 1.
    The endshift param is added to all values in column 2.
    """
    # We aim to support specification of digests by kit name
    # (potentially versioned), so this converts the kit names to the enzymes
    # used in that kit.
    make_fragment_index(output, startshift, endshift, cutshift, reference, digest)