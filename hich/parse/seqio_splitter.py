from hich.parse.file_splitter import FileSplitter
from dataclasses import dataclass, field
from Bio import SeqIO
import io
import subprocess
from smart_open import smart_open

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class SeqIOSplitter(FileSplitter):
    format_name: str = None

    def access(self, filename: str):
        if filename not in self.handles:
            self.handles[filename] = smart_open(filename, "w")
        return self.handles[filename]
    
    def write(self, filename: str, record: SeqIO.SeqRecord):
        output = io.StringIO()
        SeqIO.write(record, output, self.format_name)
        formatted_string = output.getvalue()
        output.close()
        self.access(filename).write(formatted_string)
