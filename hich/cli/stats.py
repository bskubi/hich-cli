import click
from hich.stats import DiscreteDistribution
from hich.pairs import PairsClassifier, PairsFile, PairsSegment


@click.command
@click.option("--conjuncts",
    type = str,
    default = "chr1 chr2 pair_type stratum",
    show_default = True,
    help = "PairsSegment traits that define the category for each record (space-separated string list)")
@click.option("--cis-strata",
    type = str,
    default = "",
    show_default = True,
    help = "PairsSegment cis distance strata boundaries for use with 'stratum' conjunct (space-separated string list)")
@click.option("--output",
    type = click.Path(writable=True),
    default = "",
    show_default = True,
    help = "Output file for tab-separated stats file. If not given, outputs to stdout.")
@click.argument("pairs", type = click.Path(exists=True, dir_okay=False))
def stats(conjuncts: str, cis_strata: str, output: str, pairs: str) -> None:
    """
    Classify pairs and count the events.

    Output has conjuncts as headers, one row per event, and a column "count" containing the count of each event.

    Can read 4DN .pairs format from plaintext or from a variety of compressed formats with Python's smart_open package.

    Example:
        hich stats --conjuncts "chr1 chr2" --cis-strata "10000 20000" my_pairs_file.pairs.gz
    """

    # Latest code review: 2024/10/17 - Ben Skubi

    conjuncts = conjuncts.split()

    try:
        def parse_cis_strata(cis_strata: str) -> list[int]:
            return [int(it) for it in cis_Strata.split()] if cis_strata else []
        cis_strata = parse_cis_strata(cis_strata)
    except ValueError:
        logging.error("Invalid value for --cis-strata. Ensure all elements can be parsed as integers.")
        return

    classifier = PairsClassifier(conjuncts, cis_strata)

    pairs_file = PairsFile(pairs)

    distribution = DiscreteDistribution()

    for record in pairs_file:
        outcome = classifier.classify(record)
        distribution[outcome] += 1
    
    df = classifier.to_polars(distribution)

    output_file = output or sys.stdout
    df.write_csv(output_file, separator="\t", include_header=True)