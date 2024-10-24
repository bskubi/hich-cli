from hypothesis import given, example, assume, target
from hypothesis import strategies as st
from hich.pairs import PairsSegment
from tests.utilities import cis_or_trans_chroms

@given(cis_or_trans_chroms(), st.integers(), st.integers())
def test_pairs_segment_meets_spec(chroms, pos1, pos2):
    chrom1, chrom2 = chroms
    s = PairsSegment(chrom1 = chrom1, chrom2 = chrom2, pos1 = pos1, pos2 = pos2)
    assert s.meets_spec

@given(cis_or_trans_chroms(), st.integers(), st.integers())
def test_pairs_segment_computed_correctly(chroms, pos1, pos2):
    chrom1, chrom2 = chroms
    s = PairsSegment(chrom1 = chrom1, chrom2 = chrom2, pos1 = pos1, pos2 = pos2)
    assert s.is_trans or s.distance == abs(pos1 - pos2)
