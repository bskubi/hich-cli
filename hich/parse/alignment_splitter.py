from hich.parse.file_splitter import FileSplitter
from dataclasses import dataclass, field
import pysam
import subprocess

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class AlignmentSplitter(FileSplitter):
    header: pysam.AlignmentHeader = None
    samtools_flags: str = None

    def access(self, filename: str):
        """Create/retrieve a .sam or .bam file write handle with header"""
        if filename not in self.handles:
            if self.samtools_flags is None:
                self.samtools_flags = "-b" if filename.endswith(".bam") else ""
            self.handles[filename] = subprocess.Popen(
                ["samtools", "view", self.samtools_flags, "-o", filename],
                stdin=subprocess.PIPE,
                text=True
            )
            if self.header: self.handles[filename].stdin.write(str(self.header))

        return self.handles[filename].stdin

    def write(self, filename: str, record: pysam.AlignedSegment):
        """Write a pysam AlignedSegment record to the given file"""
        self.access(filename).write(record.to_string() + "\n")