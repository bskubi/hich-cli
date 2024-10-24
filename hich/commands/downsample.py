from hich.stats import DiscreteDistribution, PairsClassifier, compute_pairs_stats_on_path, load_stats_and_classifier_from_file, aggregate_classifier
from hich.pairs import PairsFile
from hich.sample import SelectionSampler
import click

@click.command
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
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
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