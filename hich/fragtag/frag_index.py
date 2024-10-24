import numpy as np
import polars as pl

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

class FragIndex:
    def __init__(self, filename = None):
        if filename:
            self.load_bed(filename)

    def load_bed(self, filename):
        bedcols = ['chrom', 'start', 'end']
        self.chrom_intervals = pl.read_csv(filename,
                                            separator = '\t',
                                            has_header = False,
                                            new_columns = bedcols) \
                                 .sort(by=['chrom', 'start', 'end']) \
                                 .partition_by(['chrom'],
                                               as_dict = True,
                                               include_key = False)

        self.chrom_ends_sorted = {chrom:
                                  sorted(self.ends(chrom).to_list())
                                  for chrom
                                  in self.chrom_intervals}
    
    def search(self, chrom, positions):
        return np.searchsorted(self.chrom_ends_sorted[chrom], positions)
    
    def starts(self, chrom):
        return self.chrom_intervals[chrom]['start']
    
    def ends(self, chrom):
        return self.chrom_intervals[chrom]['end']
    
    def __contains__(self, chrom):
        return chrom in self.chrom_intervals