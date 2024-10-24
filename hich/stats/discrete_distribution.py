from collections import Counter
from copy import deepcopy
from functools import reduce
from numbers import Number
from scipy.optimize import linprog
from statistics import mean
import numpy as np
import random
import math

# See tests/test_discrete_distribution.py for unit tests


class DiscreteDistribution(Counter):
    @classmethod
    def mean_mass(cls, distributions: list['DiscreteDistribution']) -> 'DiscreteDistribution':
        """Compute the mean probability mass over event space.

        Sums the values assigned to each event in the sample space, defaulting to zero
        if unspecified, and divides by the total to obtain mean probability mass of each event.
        """
        # If sum() is used, it attempts to add individual values from one DiscreteDistribution
        # to another DiscreteDistribution, which isn't defined, so this is used instead.
        combined = reduce(lambda a, b: a + b, distributions)
        return combined / combined.total()
    
    @classmethod
    def bounded_probabilistic_round(cls, n, n_min, n_max):
        """Round n up or down probabilitistically within bounds (n_min, n_max)
        """
        # Testing difficulties: stochastic
        lower = max(n_min, math.floor(n)) if n_min else math.floor(n)
        upper = min(n_max, math.ceil(n)) if n_max else math.ceil(n)
        prob_up = n - lower
        if random.random() < prob_up:
            return upper
        else:
            return lower
            

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_size(self, n: Number, lower_bounds = {}, upper_bounds = {}) -> 'DiscreteDistribution':
        """Downsample to a specific number of reads (n is integer) or multiple of reads (n is float)"""
        # Testing difficulties: behaves quite differently depending on n's type 
        total = n if isinstance(n, int) else self.total() * n

        new_counts = {}
        for event, prob in self.probabilities().items():
            lower_bound = lower_bounds.get(event, 0)
            upper_bound = upper_bounds.get(event, float('inf'))
            new_counts[event] = DiscreteDistribution.bounded_probabilistic_round(prob*total, lower_bound, upper_bound)
        
        return DiscreteDistribution(new_counts)
    
    @property
    def size(self) -> Number:
        return sum(self.values())

    def downsample_to_probabilities(self, probdist: 'DiscreteDistribution') -> 'DiscreteDistribution':
        """Given a probability distribution, minimally downsample counts to match it.

        Find the maximum N such that 0 <= N*probdist[event] <= self[event] for all events
        """
        # Testing difficulty: linear programming result would have to be hand-verified

        # Cost function (linear programming attempts to minimize cost, so we
        # aim to minimize the negative sum of counts in order to maximize
        # the sum of counts)
        c = [-1]

        # Upper bounds. A_ub is a one-column vector of the probabilities.
        # b_ub is a list of the original counts, serving as the upper bound.
        # Linear programming will ensure that [A_ub]N <= b_ub

        events = self.events()
        A_ub = np.array([probdist.outcomes(events)]).T
        b_ub = list(self.outcomes(events))

        # Set lower bound of 0 on N to force positive counts
        bounds = [(0, None)]

        # Find value of N meeting criteria
        result = linprog(c, A_ub=A_ub, b_ub=[b_ub], bounds=bounds)
        
        assert result.success, "No solution found attempting to match distribution"

        # Get value of N - total number of downsampled events
        N = DiscreteDistribution.bounded_probabilistic_round(result.x[0], 0, self.size)

        return probdist.to_size(N, lower_bounds = {event: 0 for event in events}, upper_bounds = self)

    def probabilities(self) -> "DiscreteDistribution":
        return self.copy() / self.total()

    def __lt__(self, other: 'DiscreteDistribution') -> bool:
        return self.total() < other.total()

    def __le__(self, other: 'DiscreteDistribution') -> bool:
        return self.total() <= other.total()

    def __gt__(self,other: 'DiscreteDistribution') -> bool:
        return self.total() > other.total()

    def __ge__(self, other: 'DiscreteDistribution') -> bool:
        return self.total() >= other.total()

    def __add__(self, other: 'DiscreteDistribution') -> 'DiscreteDistribution':
        support = set(self.events() + other.events())
        event_outcome = {event: self[event] + other[event] for event in support}
        return DiscreteDistribution(event_outcome)
    
    def __truediv__(self, denom: Number) -> 'DiscreteDistribution':
        result = self.copy()
        for event in self.events():
            result[event] /= denom
        return result

    def copy(self) -> 'DiscreteDistribution':
        return deepcopy(self)

    def outcomes(self, events = None) -> list[Number]:
        if not events:
            return list(self.values())
        else:
            return [self[event] for event in events]

    def events(self) -> list[object]:
        return list(self.keys())