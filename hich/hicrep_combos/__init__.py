"""Run hicrep on combinations of files and parameters"""

import click
import numpy as np
from hicrep.utils import readMcool
from .hicrep_wrapper import hicrepSCC
import os
import glob
import cooler
from cooler import Cooler
from dataclasses import *
import h5py

import warnings
from pathlib import Path
from typing import Tuple, List, Callable
from collections import defaultdict
import polars as pl
from itertools import combinations, combinations_with_replacement, product, chain

from collections.abc import Iterable
import concurrent.futures
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
Get combinations of hicrep comparisons from the user, generate, and return or
save a dataframe specifying the parameters and results.

1. Get combinations of params and matrices
2. Store in polars dataframe
3. Compute SCC and distance
4. Save as tsv

"""

class SCC(float):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    def distance(self):
        (.5*(1-self))**.5 if self != np.nan else 0

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class HicrepCall:
    file1: object
    file2: object
    resolution: int
    h: int
    dBPMax: int
    bDownSample: bool
    chrom: str
    scc: SCC = None

    def cools(self):
        cool1, _ = readMcool(self.file1, self.resolution)
        cool2, _ = readMcool(self.file2, self.resolution)
        return (cool1, cool2)

    def run_hicrep(self):        
        self.scc = SCC(hicrepSCC(*self.cools(),
                                 self.h,
                                 self.dBPMax,
                                 self.bDownSample,
                                 [self.chrom]))
        return self

def run_hicrep(call):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    return call.run_hicrep()

def parallel_hicrep(callers, max_workers = None):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    with concurrent.futures.ProcessPoolExecutor(max_workers = max_workers) as executor:
        results = executor.map(run_hicrep, callers)
    if results:
        columns = list(HicrepCall.__dataclass_fields__.keys())
        rows = [astuple(result) for result in results]
        return pl.DataFrame(rows, orient='row', schema=columns)

def shared_chroms(filenames: List[str], filter = lambda chrom, size: chrom):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    all_chroms = None
    for filename in filenames:
        coolers = []
        if cooler.fileops.is_cooler(filename):
            coolers = [Cooler(filename)]
        elif cooler.fileops.is_multires_file(filename):
            coolers = [Cooler(f"{filename}::{cooler}") for cooler in cooler.fileops.list_coolers(filename)]
        else:
            raise Exception(f"shared_chroms received filename {filename} that is not cooler or multires cooler file")
        
        coolers_chromsizes = {}
        for c in coolers:
            cooler_chroms = set()
            for chrom, size in c.chromsizes.to_dict().items():
                result = filter(chrom, size)
                if result:
                    cooler_chroms.add(result)
            all_chroms = all_chroms.intersection(cooler_chroms) if all_chroms else cooler_chroms

    return all_chroms

def hicrep_callers(matrices: List,
                   resolutions: List[int],
                   chroms: List[str],
                   h: List[int],
                   dBPMax: List[int],
                   bDownSample: List[bool],
                   matrix_pair_function = combinations_with_replacement,
                   param_set_function = product):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    
    def asiterable(arg):
        return arg if isinstance(arg, Iterable) else [arg]
    
    matrices = asiterable(matrices)
    resolutions = asiterable(resolutions)
    h = asiterable(h)
    dBPMax = asiterable(dBPMax)
    bDownSample = asiterable(bDownSample)
    chroms = asiterable(chroms)

    matrix_pairs = matrix_pair_function(matrices, 2)
    param_set = param_set_function(resolutions, h, dBPMax, bDownSample, chroms)
    combos = list(product(matrix_pairs, param_set))
    
    return [HicrepCall(combo[0][0], combo[0][1], *combo[1]) for combo in combos]
    
def hicrep_combos(resolutions, chroms, exclude, chromFilter, h, d_bp_max, b_downsample, nproc, output, paths):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    assert paths, "No paths specified in hich hicrep"
    chroms = chroms or shared_chroms(paths, lambda chrom, size: eval(chromFilter))
    chroms = set(chroms) - set(exclude) if exclude else chroms
    assert chroms, "No chromosomes specified or no universally overlapping chromosomes found in hich hicrep"
    callers = hicrep_callers(paths, resolutions, chroms, h = h, dBPMax = d_bp_max, bDownSample = b_downsample)
    result = parallel_hicrep(callers, max_workers = nproc)
    if output:
        result.write_csv(output, separator = "\t")
    else:
        return result
