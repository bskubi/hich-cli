import polars as pl
import pysam
import Bio
from hich.parse.seqio_splitter import SeqIOSplitter
from hich.parse.alignment_splitter import AlignmentSplitter
from hich.parse.pairs_file import PairsFile
from hich.parse.pairs_segment import PairsSegment
from hich.parse.pairs_splitter import PairsSplitter
from hich.parse.annotation import AnnotationFile
from pathlib import Path
from collections import defaultdict

def organize(fmt: str,
             f1_path: str,
             f2_path: str = None,
             out_dir: str = None,
             annot_file: str = None,
             annot_has_header: str = False,
             annot_separator: str = "\t",
             head: int = None,
             key_code: str = None,
             record_code: str = None,
             output_code: str = None):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    def yield_none():
        while True: yield None

    parsers = {
        ("pairs",): {"file": PairsFile, "splitter": PairsSplitter},
        ("fastq", "fasta", "seqio"): {"file": Bio.SeqIO.parse, "splitter": SeqIOSplitter},
        ("sam", "bam", "sambam", "alignment"): {"file": pysam.AlignmentFile, "splitter": AlignmentSplitter}
    }
    parser = None
    for parser_key in parsers:
        if fmt in parser_key: parser = parsers[parser_key]
    
    file_type = parser["file"]
    splitter = parser["splitter"]()
    
    f1 = file_type(f1_path)
    f2 = file_type(f2_path) if f2_path else yield_none()
    
    if isinstance(splitter, PairsSplitter) or isinstance(splitter, AlignmentSplitter):
        splitter.header = f1.header

    files = zip(f1, f2)
    if key_code: key_code = compile(key_code, '<string>', 'eval')
    if record_code: record_code = compile(record_code, '<string>', 'exec')
    if output_code: output_code = compile(output_code, '<string>', 'eval')
    annotations = AnnotationFile()
    if annot_file:
        annotations.read_csv(annot_file, has_header = annot_has_header, separator = annot_separator)
    for i, records in enumerate(files):
        record = records[0]
        record1, record2 = records
        key = eval(key_code) if key_code else None
        annotation = annotations[key]
        
        if record_code: exec(record_code)
        output = eval(output_code)
        if output:
            splitter.write(output, record1)
            if record2: splitter.write(output, record2)
        if i >= head:
            break

