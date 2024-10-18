from hypothesis import strategies as st
import string
def cis_or_trans_chroms():
    chrom1 = st.text(min_size=1, max_size=10, alphabet=string.ascii_letters + string.digits)
    chrom2 = st.text(min_size=1, max_size=10, alphabet=string.ascii_letters + string.digits)
    cis = st.tuples(chrom1, chrom1)
    trans = st.tuples(chrom1, chrom2).filter(lambda pair: pair[0] != pair[1])
    return st.one_of(cis, trans)
