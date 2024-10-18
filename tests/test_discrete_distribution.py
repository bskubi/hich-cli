from hich.stats.discrete_distribution import DiscreteDistribution
import unittest

class TestDiscreteDistribution(unittest.TestCase):
    def setUp(self):
        self.is_cis = DiscreteDistribution({(True,): 10, (False,): 20})

    def test_to_size(self):
        original = DiscreteDistribution({(True,): 10, (False,): 20})
        target = DiscreteDistribution({(True,): 20, (False,): 40})
        with self.subTest(i=0):
            self.assertEqual(original.to_size(60), target)
        with self.subTest(i=1):
            self.assertEqual(original.to_size(2.0), target)

    def test_downsample_to_probabilities(self):
        original = DiscreteDistribution({(True,): 10, (False,): 40})
        probabilities = DiscreteDistribution({(True,): .1, (False,): .9})
        downsampled = original.downsample_to_probabilities(probabilities)
        self.assertIn(downsampled[(True,)], [4, 5])
        self.assertIn(downsampled[(False,)], [39, 40])

    def test_probabilities(self):
        distribution = DiscreteDistribution({True: 10, False: 30})
        probabilities = DiscreteDistribution({True: .25, False: .75})
        self.assertEqual(distribution.probabilities(), probabilities)
    
    def test_comparisons(self):
        less = DiscreteDistribution({True: 10, False: 20})
        more = DiscreteDistribution({True: 100, False: 200})
        with self.subTest(i=0):
            self.assertLess(less, more)
        with self.subTest(i=1):
            self.assertGreater(more, less)
        with self.subTest(i=2):
            self.assertLessEqual(less, more)
        with self.subTest(i=3):
            self.assertLessEqual(less, less)
        with self.subTest(i=4):
            self.assertGreaterEqual(more, less)
        with self.subTest(i=5):
            self.assertGreaterEqual(more, more)
    
    def test_add(self):
        a = DiscreteDistribution({1: 10, 2:20})
        b = DiscreteDistribution({1: 10, 3:30})
        added = DiscreteDistribution({1:20, 2:20, 3:30})
        self.assertEqual(a + b, added)
    
    def test_truediv(self):
        distribution = DiscreteDistribution({1: 10, 2: 20})
        divided = DiscreteDistribution({1: 1, 2:2})
        self.assertEqual(distribution/10, divided)

    def test_outcomes(self):
        distribution = DiscreteDistribution({1: 10, 2: 20})
        outcomes = [10, 20]
        self.assertEqual(distribution.outcomes(), outcomes)
    
    def test_events(self):
        distribution = DiscreteDistribution({1: 10, 2: 20})
        events = [1, 2]
        self.assertEqual(distribution.events(), events)

    def test_mean_mass(self):
        d1 = DiscreteDistribution({1: 10, 2: 30})
        d2 = DiscreteDistribution({1: 10, 2: 10, 3: 20})
        mean_mass = DiscreteDistribution({1: .25, 2: .5, 3: .25})
        self.assertEqual(DiscreteDistribution.mean_mass([d1, d2]), mean_mass)