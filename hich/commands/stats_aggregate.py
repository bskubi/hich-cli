import click
from hich.stats import DiscreteDistribution, compute_pairs_stats_on_path, load_stats_and_classifier_from_file, aggregate_classifier
from typing import List
from pathlib import Path

@click.command
@click.option("--to-group-mean", is_flag = True, default = False)
@click.option("--to-group-min", is_flag = True, default = False)
@click.option("--to-size", type = str, default = None)
@click.option("--prefix", type = str, default = "aggregate_")
@click.option("--outlier", type = str, multiple=True)
@click.argument("stats-paths", type = str, nargs = -1)
def stats_aggregate(to_group_mean, to_group_min, to_size, prefix, outlier, stats_paths):
    """Aggregate hich stats files called over .pairs with same conjuncts
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi

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