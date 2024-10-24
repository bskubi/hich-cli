class PairsSchema:
    
    reserved = {"readID": str, "chr1": str, "pos1": int,
                "chr2": str, "pos2": int, "strand1": str, "strand2": str}
    required = {"chr1": str, "pos1": int, "chr2": str, "pos2": int}
    synonyms = {"chrom1": "chr1", "chrom2": "chr2"}

    @classmethod
    def make_official(cls, column_name):
        return PairsSchema.synonyms.get(column_name, column_name)

    @classmethod
    def required_colnames(cls):
        return list(PairsSchema.required.keys())
    
    @classmethod
    def reserved_colnames(cls):
        return list(PairsSchema.reserved.keys())

    @classmethod
    def common_unofficial_colnames(cls):
        return list(PairsSchema.synonyms.keys())