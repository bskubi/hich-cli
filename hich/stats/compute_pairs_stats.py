from multiprocessing import Pool
from typing import Tuple
import polars as pl
from polars import DataFrame
from pathlib import Path

def compute_pairs_stats_on_path(data: Tuple["PairsClassifier", Path]) -> Tuple[str, "DiscreteDistribution"]:
    """Classify records in a PairsFile as events and return their counts
    data - a (PairsClassifier, Path) tuple for classifying PairsSegments from a PairsFile as events
    and counting the number of events.
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    from hich.stats import DiscreteDistribution
    from hich.pairs import PairsClassifier, PairsFile, PairsSegment

    classifier, pairs_path = data
    pairs_file = PairsFile(pairs_path)
    stats = DiscreteDistribution()

    # Return a count of events in the pairs file
    for record in pairs_file:        
        outcome = classifier.classify(record)
        stats[outcome] += 1
    result = (pairs_path, stats)
    return result

def aggregate_classifier(pairs_stats_paths: list[str]) -> Tuple["PairsClassifier", list["DiscreteDistribution"]]:
    """Determine the support of the combined pairs stats files

    pairs_stats_paths -- list of filenames containing pairs stats
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    from hich.pairs import PairsClassifier
    from hich.stats import DiscreteDistribution
    dfs = []
    conjuncts = None
    raw_strata = set()

    # Collect dataframes of all input stats files
    for path in pairs_stats_paths:
        df = pl.read_csv(path, separator = "\t", infer_schema_length = None)
        dfs.append(df)

        # Ensure conjuncts match for all stats files.
        new_conjuncts = [col for col in df.columns if col and col != "count"]
        conjuncts = conjuncts or new_conjuncts
        assert conjuncts == new_conjuncts, "Conjuncts do not match for all hich stats files."

        # Get unique strata listed in "stratum" column if present
        if "stratum" in conjuncts:
            strata = set(s for s in df["stratum"].unique().to_list() if s is not None)

    # Sort strata from lowest to highest; extract to integer or float.
    raw_strata = sorted(list(raw_strata))
    cis_strata = []
    for stratum in raw_strata:
        if stratum.isdigit():
            cis_strata.append(int(stratum))
        else:
            try:
                cis_strata.append(float(stratum))
            except ValueError:
                pass

    # Build a classifier using the conjuncts and complete collection of cis strata
    # Then use the classifier to convert all the dataframes into DiscreteDistributions
    classifier = PairsClassifier(conjuncts, cis_strata)
    distributions = []
    for df in dfs:
        distributions.append(classifier.from_polars(df))
    return classifier, distributions



def load_stats_and_classifier_from_file(pairs_stats_header_tsv_path):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    from hich.stats import DiscreteDistribution
    from hich.pairs import PairsClassifier, PairsFile
    df = pl.read_csv(pairs_stats_header_tsv_path, separator = "\t", infer_schema_length = None)
    conjuncts = [col for col in df.columns if col != "count"]
    cis_strata = None
    if "stratum" in conjuncts:
        raw_strata = df["stratum"].unique().to_list()
        cis_strata = []
        for s in raw_strata:
            if str(s).isdigit():
                cis_strata.append(int(s))
            elif s is not None:
                try:
                    cis_strata.append(float(str(s)))
                except ValueError:
                    print("Not integer or float", s)
                    cis_strata.append(s)
    classifier = PairsClassifier(conjuncts, cis_strata)
    distribution = DiscreteDistribution()
    for row in df.iter_rows(named = True):
        count = row["count"]
        outcome = tuple([row[conjunct] for conjunct in conjuncts])
        distribution[outcome] = count
    return classifier, distribution

def compute_pairs_stats_on_path_list(classifier, pairs_paths):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    data = [(classifier, path) for path in pairs_paths]
    return Pool().map(compute_pairs_stats_on_path, data)