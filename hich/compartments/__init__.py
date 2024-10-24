from Bio import SeqIO
import pyBigWig
import itertools
import numpy as np
import cooler
import hicstraw
from dataclasses import dataclass, field
from pathlib import Path
from cooltools.api.eigdecomp import cis_eig
from typing import List, Tuple, Dict, TextIO, TYPE_CHECKING
from scipy.sparse import coo_matrix
import polars as pl
import os
import inspect
from scipy.sparse import coo_matrix
import copy
from smart_open import smart_open

def chromsizes_hic(path: Path) -> Dict[str, int]:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    h = hicstraw.HiCFile(str(path.resolve()))
    chroms = h.getChromosomes()
    chromsizes = {}
    for c in chroms:
        chromsizes[c.name] = c.length
    return chromsizes

def chromsizes_mcool(path: Path, resolution: int) -> Dict[str, int]:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    abs_path = str(path.resolve())
    cooler_collections = cooler.fileops.list_coolers(abs_path)
    cooler_collection = f"/resolutions/{resolution}"
    assert cooler.fileops.is_multires_file(abs_path), f"{abs_path} is not a cooler multires file"
    assert cooler_collection in cooler_collections, f"{cooler_collection} is not a data collection in {abs_path}. Available collections are {cooler_collections}."
    c = cooler.Cooler(f"{abs_path}::{cooler_collection}")
    return c.chromsizes.to_dict()

def chromsizes(filename: Path, resolution: int = None) -> Dict[str, int]:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    path = Path(filename)
    assert path.exists(), f"{filename} not found at {path.resolve()}"
    
    suffix = Path(filename).suffix
    if suffix == ".mcool":
        return chromsizes_mcool(path, resolution)
    elif suffix == ".hic":
        return chromsizes_hic(path)
    raise Exception(f"Extension {suffix} not supported by hich compartments called on {path.resolve()}")

def flex_chromname(real_chromnames: List[str], chromname: str, flexible: List[str] = ["chr"]):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    for real_chromname, flex in itertools.product(real_chromnames, flexible):
        if chromname == real_chromname or chromname.replace(flex, "") == real_chromname.replace(flex, ""):
            return real_chromname
    return None

def dense_cis_count_matrix_hic(path: Path, resolution: int, chrom: str):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    h = hicstraw.HiCFile(str(path.resolve()))
    resolutions = h.getResolutions()
    chromsizes = chromsizes_hic(path)
    assert resolution in resolutions, f"Resolution {resolution} not found in {path.resolve()}. Available options are: {resolutions}"
    assert chrom in chromsizes, f"Chromosome {chrom} not found in {path.resolve()}. Available options are: {list(chromsizes.keys())}"
    
    start = 0
    end = chromsizes[chrom]
    mzd = h.getMatrixZoomData(chrom, chrom, "observed", "NONE", "BP", resolution)
    
    records = mzd.getRecords(start, end, start, end)
    row = []
    col = []
    count = []
    for r in records:
        row.append(r.binY)
        col.append(r.binX)
        count.append(r.counts)
        row.append(r.binX)
        col.append(r.binY)
        count.append(r.counts)
    row = np.array(row) // resolution
    col = np.array(col) // resolution
    return coo_matrix((count, (row, col))).toarray()
                 
def dense_cis_count_matrix_mcool(path: Path, resolution: int, chrom: str):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    abs_path = str(path.resolve())
    cooler_collections = cooler.fileops.list_coolers(abs_path)
    cooler_collection = f"/resolutions/{resolution}"
    assert cooler.fileops.is_multires_file(abs_path), f"{abs_path} is not a cooler multires file"
    assert cooler_collection in cooler_collections, f"{cooler_collection} is not a data collection in {abs_path}. Available collections are {cooler_collections}."
    c = cooler.Cooler(f"{abs_path}::{cooler_collection}")
    return c.matrix(sparse=False, balance=False).fetch(chrom)

def dense_cis_count_matrix(filename: str, resolution: int, chrom: str, flex: bool = True):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    path = Path(filename)
    assert path.exists(), f"{filename} not found at {path.resolve()}"
    
    suffix = Path(filename).suffix
    if suffix == ".mcool":
        chromnames = chromsizes_mcool(filename, resolution).keys()
        chrom = flex_chromname(chromnames, chrom) if flex else chrom
        assert chrom, f"Chrom {chrom} not found in {path}"
        return dense_cis_count_matrix_mcool(filename, resolution, chrom)
    elif suffix == ".hic":
        chromnames = chromsizes_hic(filename).keys()
        chrom = flex_chromname(chromnames, chrom) if flex else chrom
        assert chrom, f"Chrom {chrom} not found in {path}"
        return dense_cis_count_matrix_hic(filename, resolution, chrom)
    raise Exception(f"Extension {suffix} not supported by hich compartments called on {path.resolve()}")

def corr_neg(a, b):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    def ok(v):
        return not np.isnan(v) and v is not None

    keep_a = []
    keep_b = []
    for ai, bi in zip(a, b):
        if ok(ai) and ok(bi):
            keep_a.append(ai)
            keep_b.append(bi)
    
    return np.corrcoef(keep_a, keep_b)[0][1] < 0

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi
@dataclass
class BEDSignal:
    chrom: str = ""
    end: int = None
    signal: list[float] = field(default_factory = list)
    starts: list[int] = field(default_factory = list)
    ends: list[int] = field(default_factory = list)
    
    def bigwig_header(self) -> Tuple[str, int]:
        return (self.chrom, self.end)

    def add_to_bigwig(self, bw):
        assert isinstance(self.chrom, str), f"BEDSignal chrom '{self.chrom}' should be str, is {type(self.chrom)}"
        assert self.lengths_match(), f"Lengths do not match for {self}"
        
        chroms = [self.chrom]*len(self.signal)
        starts = self.starts if isinstance(self.starts, list) else self.starts.tolist()
        ends = self.ends if isinstance(self.ends, list) else self.ends.tolist()
        signal = self.signal if isinstance(self.signal, list) else self.signal.tolist()
        signal = [s if not np.isnan(s) else 0.0 for s in signal]
        for it in [chroms, starts, ends, signal]:
            print(type(it), len(it), type(it[0]), all([isinstance(i, str) or not np.isnan(i) for i in it]))
        bw.addEntries(chroms, starts, ends = ends, values = signal)

    def direct(self, vec: List[float]):
        assert len(vec) == len(self.signal), f"Signals must be same ength but were {len(vec)} and {len(self.signal)}"
        vec = vec * -1 if corr_neg(self.signal, vec) else vec
        return BEDSignal(self.chrom, self.end, vec, self.starts, self.ends)

    def lengths_match(self) -> bool:
        return len(self.signal) == len(self.starts) and len(self.signal) == len(self.ends)

    def window(self, window_size: int, endpoint: int):
        self.starts = np.arange(0, endpoint, window_size)
        self.ends = np.append((self.starts[1:] - 1), endpoint)

def gc_dist(bed, seq_iter, resolution):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    while batch := tuple(itertools.islice(seq_iter, None, resolution)):
        frac_g = (batch.count("G") + batch.count("G"))/len(batch)
        g_dist.append(frac_g)
        end = start + len(batch)
        starts.append(start)
        ends.append(end)
        start = ends[-1] + 1
    return GCDist(g_dist, starts, ends)

def frac_gc_bed(seqio_record, resolution):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    seq_iter = iter(seqio_record.seq)
    chrom = seqio_record.id
    chromsize = len(seqio_record.seq)
    
    bed = BEDSignal(chrom = chrom, end = chromsize)
    bed.window(resolution, chromsize)
    while chrom_window := tuple(itertools.islice(seq_iter, None, resolution)):
        gc_count = chrom_window.count("G") + chrom_window.count("G")
        window_size_bp = len(chrom_window)
        frac_g = gc_count / window_size_bp
        bed.signal.append(frac_g)

    assert bed.lengths_match(), f"In {chrom} at resolution {resolution}, %GC BED start, end and signal vectors do not have matching length. {bed}"
    return bed

def compartment_scores(matrix: Path, resolution: int, chrom: str, guide: BEDSignal, n_eigs: int) -> List[BEDSignal]:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    mx = dense_cis_count_matrix(matrix, resolution, chrom)
    vals, vecs = cis_eig(mx, n_eigs = n_eigs)
    return [guide.direct(vec) for vec in vecs]

def fasta_chromsizes(fasta: TextIO, chroms: List[str], exclude_chroms: List[str], keep_chroms_rule: List[str]) -> List[Tuple[str, int]]:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    sizes = []
    todo = copy.deepcopy(chroms)
    for record in SeqIO.parse(fasta, "fasta"):
        chrom = record.id
        if chroms is not None and chrom not in chroms:
            continue
        if exclude_chroms is not None and chrom in exclude_chroms:
            chroms.remove()
            continue
        if isinstance(keep_chroms_rule, str) and not eval(keep_chroms_rule):
            if todo:
                todo.remove(chrom)
            continue
        chromsize = (record.id, len(record.seq))
        sizes.append(chromsize)

        if chroms is not None:
            todo.remove(record.id)
        if chroms and not todo:
            break
    return sizes

def write_compartment_scores(bigwig_prefix: str,
                             matrix: Path,
                             reference: Path,
                             resolution: int,
                             chroms: List[str] = None,
                             exclude_chroms: List[str] = None,
                             keep_chroms_rule: str = None,
                             n_eigs: int = 3):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    ref_abs_path = str(reference.resolve())
    ref_handle = smart_open(ref_abs_path, mode = "rt")
    bw_header = fasta_chromsizes(ref_handle, chroms, exclude_chroms, keep_chroms_rule)

    bw_filenames = [f"{bigwig_prefix}_{i}.bw" for i in range(n_eigs)]
    bigwigs = [pyBigWig.open(filename, "w") for i, filename in enumerate(bw_filenames)]
    print(bw_header)
    for bw in bigwigs:
        bw.addHeader(bw_header)
    
    ref_handle.seek(0)
    todo = copy.deepcopy(chroms)
    for record in SeqIO.parse(ref_handle, "fasta"):


        chrom = record.id
        size = len(record.seq)
        
        if chroms is not None and chrom not in chroms:
            continue
        if exclude_chroms is not None and chrom in exclude_chroms:
            chroms.remove()
            continue
        if isinstance(keep_chroms_rule, str) and not eval(keep_chroms_rule):
            if todo:
                todo.remove(chrom)
            continue

        gc = frac_gc_bed(record, resolution)
        scores = compartment_scores(matrix, resolution, chrom, gc, n_eigs)
        for score, bw in zip(scores, bigwigs):
            score.add_to_bigwig(bw)
        
        if chroms and todo:
            todo.remove(chrom)
        
        if not todo:
            break

    ref_handle.close()
    for bw in bigwigs:
        print("Closing time...", type(bw))
        bw.close()
    
    print("Opening bigwigs for testing")
    bigwigs = [pyBigWig.open(filename) for i, filename in enumerate(bw_filenames)]
    print("Displaying headers")
    for bw in bigwigs:
        print("Wrote", bw.header())

