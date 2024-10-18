from hich.pairs import *
from hich.stats import DiscreteDistribution
from hypothesis import given, example, strategies as st, assume, target
from polars import DataFrame
import pickle
import unittest
from tests.utilities import cis_or_trans_chroms

class TestPairsClassifier(unittest.TestCase):
    def test_classify_with_chr1_chr2_stratum(self):
        """Test that PairsClassifier .classify method with output chromosomes and stratum bins appropriately"""

        classifier = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'], [1000, 10000])
        cis = 'chr10'
        trans1 = 'chr10'
        trans2 = 'chr11'
        with self.subTest(i=0):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 500)), (cis, cis, 1000))
        with self.subTest(i=1):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 5000)), (cis, cis, 10000))
        with self.subTest(i=2):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 50000)), (cis, cis, float('inf')))
        with self.subTest(i=3):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = -500)), (cis, cis, 1000))
        with self.subTest(i=4):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 500)), (trans1, trans2, None))
        with self.subTest(i=5):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 5000)), (trans1, trans2, None))
        with self.subTest(i=6):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 5000)), (trans1, trans2, None))

    def test_classify_with_no_record(self):
        classifier = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'], [1000, 10000])
        self.assertRaises(TypeError, classifier.classify, None)
    
    def test_classify_with_empty_record(self):
        classifier = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'], [1000, 10000])
        record = PairsSegment()
        self.assertRaises(AttributeError, classifier.classify, record)

    def test_classify_with_chr1_chr2_stratum_but_no_strata_specified(self):
        classifier = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'])
        cis = 'chr10'
        trans1 = 'chr10'
        trans2 = 'chr11'
        with self.subTest(i=0):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 500)), (cis, cis, None))
        with self.subTest(i=1):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 500)), (trans1, trans2, None))

    def test_classify_with_is_cis(self):
        classifier = PairsClassifier(['record.is_cis'])
        cis = 'chr10'
        trans1 = 'chr10'
        trans2 = 'chr11'
        with self.subTest(i=0):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 500)), (True,))
        with self.subTest(i=1):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 500)), (False,))
    
    def test_classify_with_no_conjuncts(self):
        classifier = PairsClassifier()
        record = PairsSegment(chr1 = "chr10", chr2 = "chr10", pos1 = 0, pos2 = 1000)
        self.assertRaises(TypeError, classifier.classify, record)

    def test_to_polars(self):
        classifier = PairsClassifier(['record.is_cis'])

        count = {(True,):10, (False,):20}
        distribution = DiscreteDistribution(count)
        
        target_result = DataFrame({'record.is_cis':[True, False], 'count':[10, 20]})
        
        self.assertTrue(classifier.to_polars(distribution).equals(target_result))

    def test_from_polars(self):
        from_df = DataFrame({'record.is_cis':[True, False], 'count':[10, 20]})
        classifier = PairsClassifier()
        classifier.from_polars(from_df, [1000, 10000])
        classifier = PairsClassifier(['record.is_cis'])
        cis = 'chr10'
        trans1 = 'chr10'
        trans2 = 'chr11'
        with self.subTest(i=0):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = cis, chr2 = cis, pos1 = 0, pos2 = 500)), (True,))
        with self.subTest(i=1):
            self.assertEqual(classifier.classify(PairsSegment(chr1 = trans1, chr2 = trans2, pos1 = 0, pos2 = 500)), (False,))
    
    def test_pickle_setstate_getstate(self):
        classifier = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'], [1000, 10000])
        wrong = PairsClassifier(['record.chr1', 'record.chr2', 'strat'], [1000, 10000])
        dumped = pickle.dumps(classifier)
        loaded = pickle.loads(dumped)
        with self.subTest(i=0):
            self.assertEqual(classifier, loaded)
        with self.subTest(i=1):
            self.assertNotEqual(loaded, wrong)

@given(cis_or_trans_chroms(), st.integers(), st.integers())
def test_cis_stratum(chroms, pos1, difference):
    chrom1, chrom2 = chroms

    # Set pos2 based on pos1 and the difference between pos1 and pos2
    pos2 = pos1 + difference

    # Set distance between pos1 and pos2
    distance = abs(difference)

    # For targeting strata boundaries. Zero if is_cis is False.
    # Decreases as the distance from stratum boundaries increases
    strata = [1000, 10000]
    cis_stratum1 = -abs(distance - strata[0])
    cis_stratum2 = -abs(distance - strata[1])

    # Set up Hypothesis targets and assumptions:
    # If is_cis is True, then chrom1 should equal chrom2
    # Also, target the boundaries between strata
    target(cis_stratum1, label='test_cis_stratum1')
    target(cis_stratum2, label='test_cis_stratum2')
    
    # Create PairsClassifier for chromosomes and with two cis strata
    c = PairsClassifier(['record.chr1', 'record.chr2', 'stratum'], [1000, 10000])
    s = PairsSegment(chr1 = chrom1, chr2 = chrom2, pos1 = pos1, pos2 = pos2)

    # Test cases for possible strata, cis/trans state
    if not chrom1 == chrom2:
        assert c.classify(s) == (chrom1, chrom2, None)
    elif distance <= 1000:
        assert c.classify(s) == (chrom1, chrom2, 1000)
    elif distance <= 10000:
        assert c.classify(s) == (chrom1, chrom2, 10000)
    elif distance > 10000:
        assert c.classify(s) == (chrom1, chrom2, float('inf'))
    else:
        assert False, "Weird Hypothesis test"