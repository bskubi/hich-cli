from dataclasses import dataclass, field
from smart_open import smart_open
from hich.parse.pairs_header import PairsHeader
from hich.parse.pairs_segment import PairsSegment
from pathlib import PurePath, Path

@dataclass
class PairsFile:
    filepath_or_object: str = None
    mode: str = None
    header: PairsHeader = field(default_factory = PairsHeader)

    def __init__(self, filepath_or_object, mode = "rt", header = None):
        self.open(filepath_or_object, mode, header)
        self.duration = 0

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self.filepath_or_object, "close"):
            self.filepath_or_object.close()

    def open(self, filepath_or_object, mode = "r", header = None):
        # Close previously open file if necessary and set read/write/append mode
        self.close()
        
        self.mode = mode

        is_path = isinstance(filepath_or_object, (str, PurePath))
        exists = is_path and Path(filepath_or_object).exists()
        
        if is_path:
            self.filepath_or_object = smart_open(filepath_or_object, mode = self.mode)
        else:
            self.filepath_or_object = filepath_or_object

        if "r" in self.mode:
            self.read_header(header)
        elif "w" in self.mode or "a" in self.mode:
            self.header = header
            file_is_empty = ("a" in self.mode and self.filepath_or_object.seekable() and self.filepath_or_object.tell() == 0) or "w" in self.mode
            if file_is_empty:
                self.filepath_or_object.write(self.header.to_string())

    def read_header(self, header: PairsHeader = None):
        if isinstance(header, PairsHeader):
            self.header = header
            return
        file_like = hasattr(self.filepath_or_object, "seek") and hasattr(self.filepath_or_object, "readline")
        assert file_like, f"PairsFile filepath_or_object {self.filepath_or_object} must have tell, seek and readline methods"
        self.filepath_or_object.seek(0)

        lines = []
        while True:
            pos = self.filepath_or_object.tell()
            line = self.filepath_or_object.readline()
            if not line.startswith("#"):
                self.filepath_or_object.seek(pos)
                break
            else:
                lines.append(line)
        header_text = "".join(lines)
        self.header = PairsHeader.from_text(header_text)

    def pair_segment_from_text(self, line):
        import time
        
        # From here...
        stripped = line.strip()
        if not stripped:
            raise StopIteration
        fields = stripped.split()
        field_vals = {self.indexed_columns[idx]: val for idx, val in enumerate(fields)}
        # .. to here takes about 0.74 seconds

        
        # This statement takes about 2.3 seconds
        record = PairsSegment(**field_vals)
        
        return record

    def __iter__(self):
        self.indexed_columns = dict(enumerate(self.header.columns))
        return self

    def __next__(self):
        line = self.filepath_or_object.readline()
        record = self.pair_segment_from_text(line)
        return record


    def to_header(self):
        self.filepath_or_object.seek(0)
    
    def to_records(self, record_number=0):
        self.to_header()

        # First, scan for the first non-comment line
        while True:
            position = self.filepath_or_object.tell()  # Get the current file pointer position
            line = self.filepath_or_object.readline()  # Read a line manually

            if not line:  # If we reach EOF, exit the loop
                break

            if not line.startswith("#"):
                self.filepath_or_object.seek(position)  # Seek back to the start of the non-comment line
                if record_number == 0:
                    return line

        # If record_number is not 0, continue reading lines
        while record_number > 0:
            line = self.filepath_or_object.readline()
            record_number -= 1
            if not line:  # Handle the case where EOF is reached
                return None
            if record_number == 0:
                return line

    def write(self, pairs_segment: PairsSegment):
        self.filepath_or_object.write("\n" + pairs_segment.to_string())
