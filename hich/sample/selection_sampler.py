from hich.stats.discrete_distribution import DiscreteDistribution
from dataclasses import dataclass, field
from random import random

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class SelectionSampler:
    full: DiscreteDistribution = field(default_factory = DiscreteDistribution)
    target: DiscreteDistribution = field(default_factory = DiscreteDistribution)
    viewed: DiscreteDistribution = field(default_factory = DiscreteDistribution)
    kept: DiscreteDistribution = field(default_factory = DiscreteDistribution)

    def count(self, event):
        self.full[event] += 1

    def sample(self, event):
        to_view = self.full[event] - self.viewed[event]
        to_sample = self.target[event] - self.kept[event]
        keep = to_view * random() < to_sample
        self.kept[event] += keep
        self.viewed[event] += 1
        return keep

    
