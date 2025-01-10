import click
from hich.stats import DiscreteDistribution
from hich.pairs import PairsClassifier, PairsFile, PairsSegment
from hich.cli import IntList, StrList
from hich.io import df_to_disk_or_stdout

def count_pairs_stats(classifier: PairsClassifier, pairs_file: PairsFile) -> DiscreteDistribution:
    """Get a DiscreteDistribution of counts of pairs in the pairs_file classified as events"""

    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    distribution = DiscreteDistribution()

    for record in pairs_file:
        outcome = classifier.classify(record)
        distribution[outcome] += 1
    return distribution

@click.command
@click.option("--conjuncts",
    type = StrList,
    default = "chrom1, chrom2, pair_type, stratum",
    show_default = True,
    help = "PairsSegment traits that define the category for each record (comma-separated string list)")
@click.option("--cis-strata",
    type = IntList,
    default = "",
    show_default = True,
    help = "PairsSegment cis distance strata boundaries for use with 'stratum' conjunct (comma-separated string list)")
@click.option("--output",
    type = click.Path(writable=True),
    default = "",
    show_default = True,
    help = "Output file for tab-separated stats file. If not given, outputs to stdout.")
@click.argument("pairs", type = click.Path(exists=True, dir_okay=False))
def stats(conjuncts: str, cis_strata: IntList, output: click.Path, pairs: click.Path) -> None:
    """
    Classify pairs and count the events.

    Output has conjuncts as headers, one row per event, and a column "count" containing the count of each event.

    Can read 4DN .pairs format from plaintext or from a variety of compressed formats with Python's smart_open package.

    Example:
        hich stats --conjuncts "chr1 chr2" --cis-strata "10000 20000" my_pairs_file.pairs.gz
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi

    classifier = PairsClassifier(conjuncts, cis_strata)
    pairs_file = PairsFile(pairs)
    distribution = count_pairs_stats(classifier, pairs_file)
    df = classifier.to_polars(distribution)
    df_to_disk_or_stdout(df, output, include_header=True, separator = "\t")