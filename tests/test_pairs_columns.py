from hich.parse.pairs_columns import PairsColumns
from hich.parse.pairs_header_line import PairsHeaderLine

def test_pairs_columns():
    assert PairsColumns.from_columns_line("#columns: chrom1 pos1 chrom2 pos2") == ["chr1", "pos1", "chr2", "pos2"]

def test_pairs_header_lines():
    PairsHeaderLine(line = "#header_line")
    PairsHeaderLine(line = "bad_header_line")