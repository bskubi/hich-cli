from collections import defaultdict
from hich.cli import BooleanList, IntList, PathList, StrList
from hich.compartments import write_compartment_scores
from hich.fragtag import tag_restriction_fragments
from hich.hicrep_combos import hicrep_combos
from hich.pairs import PairsClassifier, PairsFile
from hich.pairs import read_pairs
from hich.sample import SelectionSampler
from hich.stats import DiscreteDistribution, compute_pairs_stats_on_path, load_stats_and_classifier_from_file, aggregate_classifier
from hich.visuals import view_hicrep
from itertools import combinations
from numbers import Number
from pathlib import Path
from polars import DataFrame
from polars import DataFrame
from smart_open import smart_open
from typing import Union
import click
import click
import hich.digest as _digest
import io
import logging
import pandas as pd
#from parse import parse as _parse
import polars as pl
import polars as pl
import smart_open_with_pbgzip
import sys

@click.group
def hich():
    pass

@hich.command
@click.option("--chroms", type = StrList, default = None)
@click.option("--exclude-chroms", type = StrList, default = None)
@click.option("--keep-chroms-when", type = str, default = None)
@click.option("--n_eigs", type = int, default = 1)
@click.argument("reference")
@click.argument("matrix")
@click.argument("resolution", type = int)
def compartments(chroms, exclude_chroms, keep_chroms_when, n_eigs, reference, matrix, resolution):
    matrix = Path(matrix)
    reference = Path(reference)
    final_suffix = matrix.suffixes[-1]
    prefix = matrix.name.rstrip(final_suffix)

    write_compartment_scores(prefix, matrix, reference, resolution, chroms, exclude_chroms, keep_chroms_when, n_eigs)

@hich.command
@click.option("--conjuncts",
    type = str,
    default = "record.chr1 record.chr2 record.pair_type stratum",
    show_default = True,
    help = "PairsSegment traits that define the category for each record (space-separated string list)")
@click.option("--cis-strata",
    type = str,
    default = "10 20 50 100 200 500 1000 2000 5000 10000 20000 50000 100000 200000 500000 1000000 2000000 5000000",
    show_default = True,
    help = "PairsSegment cis distance strata boundaries (space-separated string list)")
@click.option("--orig-stats",
    type = str,
    default = "",
    show_default = True,
    help = ("Stats file containing original count distribution. Can be produced with hich stats. "
            "Computed from conjuncts and cis_strata if not supplied. Overrides default conjuncts and cis_strata if they are supplied."))
@click.option("--target-stats",
    type = str,
    default = "",
    show_default = True,
    help = "Stats file containing target count distribution.")
@click.option("--to-size",
    type = str,
    default = "",
    show_default = True,
    help = ("Float on [0.0, 1.0] for fraction of records to sample, or positive integer number of counts to sample. "
            "If a target stats file is supplied, further downsamples it to the given count."))
@click.argument("input_pairs_path", type = str)
@click.argument("output_pairs_path", type = str)
def downsample(conjuncts, cis_strata, orig_stats, target_stats, to_size, input_pairs_path, output_pairs_path):
    orig_classifier, orig_distribution = load_stats_and_classifier_from_file(orig_stats) if orig_stats else (None, None)
    target_classifier, target_distribution = load_stats_and_classifier_from_file(target_stats) if target_stats else (None, None)
    
    if orig_classifier and target_classifier:
        assert orig_classifier.conjuncts == target_classifier.conjuncts, f"Original and target conjuncts do not match for {orig_stats} and {target_stats}."
        conjuncts = orig_classifier.conjuncts
        if "stratum" in orig_classifier.conjuncts and "stratum" in target_classifier.conjuncts:
            cis_strata = list(set(orig_classifier.cis_strata + target_classifier.cis_strata))        
    elif orig_classifier:
        conjuncts = orig_classifier.conjuncts
    elif target_classifier:
        conjuncts = target_classifier.conjuncts
    
    classifier = PairsClassifier(conjuncts, cis_strata)
    
    if to_size.isdigit():
        to_size = int(to_size)
    else:
        try:
            to_size = float(to_size)
        except ValueError:
            to_size = None
    
    if not orig_distribution:
        _, orig_distribution = compute_pairs_stats_on_path((classifier, pairs_path))
    if not target_distribution:
        assert to_size is not None, "No target distribution or count supplied for downsampling."
        target_distribution = orig_distribution.to_size(to_size)
    if to_size:
        target_distribution = target_distribution.to_size(to_size)
    sampler = SelectionSampler(full = orig_distribution, target = target_distribution)
    input_pairs_file = PairsFile(input_pairs_path)
    output_pairs_file = PairsFile(output_pairs_path, mode = "w", header = input_pairs_file.header)

    for record in input_pairs_file:
        outcome = classifier.classify(record)
        if sampler.sample(outcome):
            output_pairs_file.write(record)

@hich.command()
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
    _digest.digest(output, startshift, endshift, cutshift, reference, digest)

@hich.command
@click.option("--batch_size", default = 1000000)
@click.argument("fragfile")
@click.argument("out_pairs")
@click.argument("in_pairs")
def fragtag(batch_size, fragfile, out_pairs, in_pairs):
    tag_restriction_fragments(fragfile, in_pairs, out_pairs, batch_size)

@hich.command
@click.option("--format", "--fmt", "fmt",
    type = click.Choice(['fasta', 'fastq', 'seqio', 'sam', 'bam', 'sambam', 'alignment', 'pairs']), required = True,
    help = "Alignment data format")
@click.option("--f1", "-f", "--file", "--file1", type = str, required = True,
    help = "Path to first (or only) input sequencing data file")
@click.option("--f2", "--file2", "f2", default = None, type = str, show_default = True,
    help = "Path to second input sequencing data file")
@click.option("--out-dir", "--dir", "--output-dir", type = str, default = "",
    help = "Output directory")
@click.option("--annot-file", "--annot", "-a", "--annotations", default = None, type = str,
    help = ("Path to annotation file, a columnar text file mapping data file "
            "information such as a cell barcode to new information such as "
            "an experimental condition or cell ID. Annotation files with headers "
            "convert to a dict with format {col1_row: {col2:col2_row, col3:col3_row...}}."))
@click.option("--annot-has-header", "-h", type = bool, default = False, show_default = True,
    help = "Whether or not annotation file has a header row.")
@click.option("--annot-separator", "-s", type = str, default = "\t", show_default = repr('\t'),
    help = "The column separator character")
@click.option("--head", "--n_records", type = int, default = None, show_default = True,
    help = "Take only the first n records from the data files. Takes all records if not provided.")
@click.option("--key-code", "--kc", type = str, default = None,
    help = "Python code to extract an annotation row key from the current record.")
@click.option("--record-code", "--rc", type = str, default = None,
    help = "Python code to modify the record")
@click.option("--output-code", "--fc", "--filename", type = str, default = None,
    help = "Python code to select the record output.")
def organize(fmt,
             f1,
             f2,
             out_dir,
             annot_file,
             annot_has_header,
             annot_separator,
             head,
             key_code,
             record_code,
             output_code):
    """Reannotate and split sequencing data files


    """
    raise NotImplementedError("Hich organize is not implemented yet")

@hich.command
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
    result = hicrep_combos(resolutions, chroms, exclude, chrom_filter, h, d_bp_max, b_downsample, nproc, output, paths)
    if result is not None:
        click.echo(result)

@hich.command()
@click.option("--read_from", type=str, default="")
@click.option("--output_to", type=str, default="")
@click.option("--parse", type=str, nargs = 3, multiple=True, default=[], help="Format is --update [FROM_COL] [TO_COL] '[PATTERN]' as in --pattern 'readID' 'cellID' '{cellID}:{ignore}'")
@click.option("--placeholder", type=str, nargs = 2, multiple=True, default=[], help="Format is --placeholder [COL] [PLACEHOLDER] which replaces every column value with the placeholder string")
@click.option("--regex", type=str, nargs = 4, multiple=True, default=[], help="Format is --placeholder [FROM_COL] [TO_COL] [REGEX] [GROUP_INDEX] which extracts the group index specified (0=whole pattern) from the given regex from FROM_COL and sets it as the value in TO_COL")
@click.option("--drop", type=str, multiple=True, default=[], help="Column to drop")
@click.option("--select", type=str, default = "", help="Space-separated list of output column names to output in the order specified")
@click.option("--batch-size", type=int, default=10000, help="Number of records per batch")
def reshape(read_from, output_to, parse, placeholder, regex, drop, select, batch_size):
    read_from = smart_open(read_from, "rt") if read_from else sys.stdin
    reader = read_pairs(read_from, yield_columns_line=False, batch_size=batch_size)
    header = next(reader)
    output = smart_open(output_to, "wt") if output_to else None
    parse_cols = [
        (pl.col(from_col)
           .map_elements(lambda x: _parse(pattern, x)[to_col], return_dtype=pl.String)
           .alias(to_col))
        for from_col, to_col, pattern in parse
    ]
    regex_cols = [
        pl.col(from_col).str.extract(pattern, int(group_index)).alias(to_col)
        for from_col, to_col, pattern, group_index in regex
    ]
    placeholder_cols = [
        pl.lit(lit).alias(col)
        for col, lit in placeholder
    ]
    update_cols = parse_cols + regex_cols + placeholder_cols
    select = select.split()
    
    header_written = False

    if output:
        output.write(header)
    else:
        click.echo(header, nl=False)
    for df in reader:
        df = df.with_columns(*update_cols) if update_cols else df
        df = df.drop(drop) if drop else df
        df = df.select(select) if select else df
        if output:
            if not header_written:
                output.write("#columns: " + " ".join(df.columns) + "\n")
                header_written = True
            df = df.to_pandas()
            df.to_csv(output, sep="\t", header=False, index=False)
        else:
            df = df.to_pandas()
            buffer = io.StringIO()
            if not header_written:
                click.echo("#columns: " + " ".join(df.columns) + "\n", nl=False)
                header_written = True

            df.to_csv(buffer, sep="\t", header=False, index=False)
            click.echo(buffer.getvalue(), nl=False)

@hich.command
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
    from hich.stats import DiscreteDistribution
    from hich.pairs import PairsClassifier, PairsFile, PairsSegment

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


@hich.command
@click.option("--to-group-mean", is_flag = True, default = False)
@click.option("--to-group-min", is_flag = True, default = False)
@click.option("--to-size", type = str, default = None)
@click.option("--prefix", type = str, default = "aggregate_")
@click.option("--outlier", type = str, multiple=True)
@click.argument("stats-paths", type = str, nargs = -1)
def stats_aggregate(to_group_mean, to_group_min, to_size, prefix, outlier, stats_paths):
    """Aggregate hich stats files called over .pairs with same conjuncts
    """

    # Load the stats files into dataframes.
    # Ensure the stats files have identical conjuncts.
    # If they have a stratum column, aggregate all the unique strata from each
    # stratum column to get the complete collection of strata.
    # Use the classifier to build DiscreteDistributions from the dataframes.
    classifier, distributions = aggregate_classifier(stats_paths)

    # Get the complete collection of distributions.
    targets = [d for d in distributions]

    build_prefix = ""
    if to_group_mean:
        # Get the mean probability mass for all distributions.
        non_outliers = [distribution for distribution, path in zip(distributions, stats_paths) if path not in outlier]
        group_mean = DiscreteDistribution.mean_mass(non_outliers)
        
        # Downsample each individual sample by the minimum amount necessary to match its mean probabilities for the group.
        targets = [d.downsample_to_probabilities(group_mean) for d in distributions]
        if prefix is None:
            build_prefix += "to_group_mean"
    if to_group_min:
        # Downsample all samples to the size of the smallest one.
        # Then downsample all samples to that size.
        min_size: int = min(targets).total()
        targets: List[DiscreteDistribution] = [d.to_size(min_size) for d in targets]

        if prefix is None:
            build_prefix += "to_group_min"
    if to_size:
        if to_size.isdigit():
            to_size = int(to_size)
        else:
            to_size = float(to_size)
        targets = [d.to_size(to_size) for d in targets]
        if prefix is None:
            build_prefix += f"to_{to_size}"
    if prefix is None:
        prefix = build_prefix + "_"
    for d, stats_path in zip(targets, stats_paths):
        df = classifier.to_polars(d)
        path = str(Path(stats_path).parent / (prefix + Path(stats_path).name))
        df.write_csv(path, separator = "\t", include_header = True)
    

@hich.group 
def view(): pass

@view.command(name='hicrep')
@click.option("--host", default = "127.0.0.1", show_default = True)
@click.option("--port", default = 8050, show_default = True)
def hicrep_comparisons(host, port):
    view_hicrep.run_dashboard(host, port)

if __name__ == "__main__":
    hich()
