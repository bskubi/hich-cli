from dataclasses import dataclass
from smart_open import smart_open
from hich.parse.pairs_header import PairsHeader
from hich.parse.pairs_segment import PairsSegment
from hich.parse.pairs_file import PairsFile
from hich.parse.file_splitter import FileSplitter

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class PairsSplitter(FileSplitter):
    """ Write to 4DN Consortium .pairs-format files, creating handles as needed
    """
    header: PairsHeader = None

    def access(self, filename: str):
        if filename not in self.handles:
            self.handles[filename] = PairsFile(filename, "w", header = self.header)
        return self.handles[filename]
    
    def write(self, filename: str, pairs_segment: PairsSegment):
        self.access(filename).write(pairs_segment)
