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

def tag_restriction_fragments(frags_filename: str,
                              input_pairs_filename: str,
                              output_pairs_filename: str,
                              batch_size: int = 1000000):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    
    frag_index = FragIndex(frags_filename)
    pairs_parser = PairsParser(input_pairs_filename)

    for df in pairs_parser.batch_iter(batch_size):  
        df = BedpePairs(df).fragtag(frag_index)

        pairs_parser.write_append(output_pairs_filename,
                                  df,
                                  header_end = SamheaderFragtag())


    pairs_parser.close()

