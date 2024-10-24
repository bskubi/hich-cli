from hicrep.hicrep import sccByDiag
from hicrep.utils import *
import scipy.sparse as sp

def cool2pixels(cool: cooler.api.Cooler):
    """Return the contact matrix in "pixels" format

    Args:
        cool: Input cooler object

    Returns:
        cooler.core.RangeSelector2D object
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    return cool.matrix(as_pixels=True, balance=True, sparse=True)



def hicrepSCC(cool1: cooler.api.Cooler, cool2: cooler.api.Cooler,
              h: int, dBPMax: int, bDownSample: bool,
              chrNames: list = None, excludeChr: set = None) -> dict:
    """Compute hicrep score between two input Cooler contact matrices

    Args:
        cool1: `cooler.api.Cooler` Input Cooler contact matrix 1
        cool2: `cooler.api.Cooler` Input Cooler contact matrix 2
        h: `int` Half-size of the mean filter used to smooth the
        input matrics
        dBPMax `int` Only include contacts that are at most this genomic
        distance (bp) away
        bDownSample: `bool` Down sample the input with more contacts
        to the same number of contacts as in the other input
        chrNames: `list` List of chromosome names whose SCC to
        compute. Default to None, which means all chromosomes in the
        genome are used to compute SCC
        excludeChr: `set` Set of chromosome names to exclude from SCC
        computation. Default to None.

    Returns:
        `float` scc scores for each chromosome
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    
    # Get binsize for each contact matrix
    binSize1 = cool1.binsize
    binSize2 = cool2.binsize

    # Input validation:
    # Same bin sizes and number of bins
    # Same number of chroms
    # Same chrom names
    
    assert binSize1 == binSize2,\
        f"Input cool files have different bin sizes"
    assert coolerInfo(cool1, 'nbins') == coolerInfo(cool2, 'nbins'),\
        f"Input cool files have different number of bins"
    assert coolerInfo(cool1, 'nchroms') == coolerInfo(cool2, 'nchroms'),\
        f"Input cool files have different number of chromosomes"
    assert (cool1.chroms()[:] == cool2.chroms()[:]).all()[0],\
        f"Input file have different chromosome names"
    
    
    binSize = binSize1
    bins1 = cool1.bins()
    bins2 = cool2.bins()
    
    # Use median bin size if bin size is non-uniform
    if binSize is None:
        # sometimes bin size can be None, e.g., input cool file has
        # non-uniform size bins.
        assert np.all(bins1[:] == bins2[:]),\
            f"Input cooler files don't have a unique bin size most likely "\
            f"because non-uniform bin size was used and the bins are defined "\
            f"differently for the two input cooler files"
        # In that case, use the median bin size
        binSize = int(np.median((bins1[:]["end"] - bins1[:]["start"]).values))
        warnings.warn(f"Input cooler files don't have a unique bin size most "\
                      f"likely because non-uniform bin size was used. HicRep "\
                      f"will use median bin size from the first cooler file "\
                      f"to determine maximal diagonal index to include", RuntimeWarning)
    
    # Validate dBPMax
    
    if dBPMax == -1:
        # this is the exclusive upper bound
        dMax = coolerInfo(cool1, 'nbins')
    else:
        dMax = dBPMax // binSize + 1

    assert dMax > 1, f"Input dBPmax is smaller than binSize"
    
    # Get pixels
    p1 = cool2pixels(cool1)
    p2 = cool2pixels(cool2)
    
    # get the total number of contacts as normalizing constant
    n1 = coolerInfo(cool1, 'sum')
    n2 = coolerInfo(cool2, 'sum')

    # Use dict here so that the chrNames don't duplicate
    if chrNames is None:
        chrNamesDict = dict.fromkeys(cool1.chroms()[:]['name'].tolist())
    else:
        chrNamesDict = dict.fromkeys(chrNames)
    
    # It's important to preserve the order of the input chrNames so that the
    # user knows the order of the output SCC scores so we bail when encounter
    # duplicate names rather than implicit prunning the names.
    assert chrNames is None or len(chrNamesDict) == len(chrNames), f"""
        Found Duplicates in {chrNames}. Please remove them.
        """
    # filter out excluded chromosomes
    if excludeChr is None:
        excludeChr = set()

    chrNames = [ chrName for chrName in chrNamesDict if chrName not in excludeChr ]

    # Create scc score vector with placeholder value
    scc = np.full(len(chrNames), -2.0)

    # Compute scc scores
    for iChr, chrName in enumerate(chrNames):
        # normalize by total number of contacts
        mS1 = getSubCoo(p1, bins1, chrName)
        
        # Input validation on shape of chromosome matrix
        assert mS1.size > 0, "Contact matrix 1 of chromosome %s is empty" % (chrName)
        assert mS1.shape[0] == mS1.shape[1],\
            "Contact matrix 1 of chromosome %s is not square" % (chrName)
        
        mS2 = getSubCoo(p2, bins2, chrName)
        assert mS2.size > 0, "Contact matrix 2 of chromosome %s is empty" % (chrName)
        assert mS2.shape[0] == mS2.shape[1],\
            "Contact matrix 2 of chromosome %s is not square" % (chrName)
        assert mS1.shape == mS2.shape,\
            "Contact matrices of chromosome %s have different input shape" % (chrName)
        try:
            scc[iChr] = computeSCC(mS1, mS2, n1, n2, dMax, h, bDownSample)
        except Exception as e:
            print(e)

    return scc

def computeSCC(mS1: sp.coo_matrix,
               mS2: sp.coo_matrix,
               n1: float,
               n2: float,
               dMax: int,
               h: float,
               bDownSample: bool) -> float:
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    # Compute scc score
    nDiags = mS1.shape[0] if dMax < 0 else min(dMax, mS1.shape[0])
    rho = np.full(nDiags, np.nan)
    ws = np.full(nDiags, np.nan)
    # remove major diagonal and all the diagonals >= nDiags
    # to save computation time
    m1 = trimDiags(mS1, nDiags, False)
    m2 = trimDiags(mS2, nDiags, False)
    del mS1
    del mS2
    if bDownSample:
        # do downsampling
        size1 = m1.sum()
        size2 = m2.sum()
        if size1 > size2:
            m1 = resample(m1, size2).astype(float)
        elif size2 > size1:
            m2 = resample(m2, size1).astype(float)
    else:
        # just normalize by total contacts
        m1 = m1.astype(float) / n1
        m2 = m2.astype(float) / n2
    if h > 0:
        # apply smoothing
        m1 = meanFilterSparse(m1, h)
        m2 = meanFilterSparse(m2, h)

    return sccByDiag(m1, m2, nDiags)