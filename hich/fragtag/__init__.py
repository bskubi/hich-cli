import click
import numpy as np
import polars as pl
import time
import warnings
import sys
from hich.parse.pairs_parser import PairsParser
from hich.fragtag.frag_index import FragIndex
from hich.fragtag.bedpe_pairs import BedpePairs
from hich.fragtag.samheader_fragtag import SamheaderFragtag

def tag_batch(pairs_batch_df, frag_index):
    pairs_batch_df = tag_with_pair_id(pairs_batch_df)
    ends = ends_format(pairs_batch_df)
    chroms_dict = partition_by_chrom(ends)

    for chrom, chrom_df in chroms_dict.items():
        frag_columns = format_frag_columns(frag_index,
                                            chrom,
                                            chrom_df)
        chroms_dict[chrom] = chroms_dict[chrom].with_columns(frag_columns)
    
    return pairsFormat(pairs_batch_df, chroms_dict)

def tag_restriction_fragments(frags_filename: str,
                              input_pairs_filename: str,
                              output_pairs_filename: str,
                              batch_size: int = 1000000):
    
    frag_index = FragIndex(frags_filename)

    pairs_parser = PairsParser(input_pairs_filename)

    for df in pairs_parser.batch_iter(batch_size):  
        df = BedpePairs(df).fragtag(frag_index)

        pairs_parser.write_append(output_pairs_filename,
                                  df,
                                  header_end = SamheaderFragtag())


    pairs_parser.close()
