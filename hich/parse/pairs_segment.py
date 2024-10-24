import time

# See tests/test_pairs_segment.py for unit tests

class PairsSegment:
    reserved = {"readID": str, "chr1": str, "pos1": int,
                "chr2": str, "pos2": int, "strand1": str, "strand2": str}
    required = {"chr1": str, "pos1": int, "chr2": str, "pos2": int}
    alt = {"chrom1":"chr1", "chrom2":"chr2"}

    def __init__(self, **kwargs):
        self.__dict__ = kwargs
        self.cast_reserved()
        self.alt_to_main()
    
    def cast_reserved(self):
        self.pos1 = int(self.pos1) if "pos1" in self.__dict__ else 0
        self.pos2 = int(self.pos2) if "pos2" in self.__dict__ else 0
    
    def alt_to_main(self):
        for alt, main in PairsSegment.alt.items():
            # Access self.__dict__ directly for faster lookups
            alt_val = self.__dict__.get(alt)
            main_val = self.__dict__.get(main)

            if alt_val is None and main_val is not None:
                self.__dict__[alt] = main_val
            elif main_val is None and alt_val is not None:
                self.__dict__[main] = alt_val
    

    def to_dict(self, columns = None):
        if columns:
            return {c:self.__dict__[c] for c in columns if c in self.__dict__}
        else:
            reserved = {k:self.__dict__[k] for k in PairsSegment.reserved
                 if k in self.__dict__}
            non_reserved = {k:v for k, v in self.__dict__.items()
                    if k not in PairsSegment.alt
                    and k not in reserved}
            reserved.update(non_reserved)
            return reserved

    def to_string(self, columns = None):
        return "\t".join(str(v) for v in self.to_dict(columns).values())

    @property
    def distance(self):
        return abs(self.pos1 - self.pos2) if hasattr(self, 'chr1') and self.is_cis else None
    
    @property
    def meets_spec(self):
        return all([hasattr(self, requirement) for requirement in PairsSegment.required])

    @property
    def is_cis(self): return self.chr1 == self.chr2
    
    @property
    def is_trans(self): return self.chr1 != self.chr2

    @property
    def intrachr(self): return self.is_cis

    @property
    def interchr(self): return self.is_trans

    @property
    def is_ur(self): return self.pair_type in ["UU", "RU", "UR"]

    def __str__(self): return self.to_string()