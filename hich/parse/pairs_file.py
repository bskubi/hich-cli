from dataclasses import dataclass, field
from smart_open import smart_open
from hich.parse.pairs_header import PairsHeader
from hich.parse.pairs_segment import PairsSegment
from pathlib import PurePath, Path

@dataclass
class PairsFile:
    """Parser for 4DN .pairs format

    # 4DN .pairs file spec
    # https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md
    """

    filepath_or_object: str = None
    mode: str = None
    header: PairsHeader = PairsHeader()

    def __init__(
            self, 
            filepath_or_object: Union[str, PurePath], 
            mode: str = "rt", 
            header: PairsHeader = None):
        """Create a PairsFile object to facilitate parsing 4DN .pairs

        Args:
            filepath_or_object (Union[str, PurePath]): String or Path to parse
            mode (str, optional): open mode. Defaults to "rt".
            header (PairsHeader, optional): substitute a preconstructed PairsHeader; constructed from file if None. Defaults to None.
        """
        self.open(filepath_or_object, mode, header)

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        """Close any open file
        """
        if hasattr(self.filepath_or_object, "close"):
            self.filepath_or_object.close()

    def open(
            self, 
            filepath_or_object, 
            mode: str = "r", 
            header: PairsHeader = None
            ) -> None:
        """Open the file with the chosen mode and construct/store the header

        Args:
            filepath_or_object: Either a path to a file or an Iterable that can be read from like a file
            mode (str, optional): Mode in which to open it. Defaults to "r".
            header (PairsHeader, optional): Optional preconstructed header. Defaults to None.
        """
        # Close previously open file if necessary and set read/write/append mode
        self.close()
        
        # Open mode (i.e. r, w, a)
        self.mode = mode

        # Check if the filepath or object needs to be opened and exists
        is_path = isinstance(filepath_or_object, (str, PurePath))

        # Open the file or store it if it is not a path
        if is_path:
            self.filepath_or_object = smart_open(filepath_or_object, mode = self.mode)
        else:
            self.filepath_or_object = filepath_or_object

        # Read in the header if not given, or write the preconstructed header if the file is empty
        if "r" in self.mode:
            self.read_header(header)
        elif "w" in self.mode or "a" in self.mode:
            self.header = header

            # Write the header if the file is empty
            at_start = self.filepath_or_object.seekable() and self.filepath_or_object.tell() == 0
            file_is_empty = ("a" in self.mode and at_start) or "w" in self.mode
            if file_is_empty:
                self.filepath_or_object.write(self.header.to_string())

    def read_header(self, header: PairsHeader = None) -> PairsHeader:
        """_summary_

        Args:
            header (PairsHeader, optional): _description_. Defaults to None.
        """
        # Set the header if given
        if isinstance(header, PairsHeader):
            self.header = header
        else:
            # Check if we have the methods needed to read a header from the filepath_or_object
            file_like = hasattr(self.filepath_or_object, "seek") and hasattr(self.filepath_or_object, "readline")
            assert file_like, f"PairsFile filepath_or_object {self.filepath_or_object} must have tell, seek and readline methods"

            # Go to the beginning of the file and read the header
            self.filepath_or_object.seek(0)

            # We have to read through the .pairs file header lines until reading the first one that does not start with #
            # At that point we need to return to the start of this first non-header line
            lines = []
            while True:
                # Store the current position
                pos = self.filepath_or_object.tell()

                # Read a line (moving the current position ahead)
                line = self.filepath_or_object.readline()

                if not line.startswith("#"):
                    # Found the first nonheader line, so go back to the start of it
                    self.filepath_or_object.seek(pos)
                    break
                else:
                    # Found another header line, add it to the list
                    lines.append(line)

            # Join the entire header together into a single string
            header_text = "".join(lines)

            # Parse the raw header text into a PairsHeader object
            self.header = PairsHeader.from_text(header_text)

    def pair_segment_from_text(self, line: str) -> PairsSegment:
        """Parse a line from the data section of a pairs file into a PairSegment object     

        Args:
            line (str): A line from the data section of a .pairs file

        Returns:
            PairsSegment: A PairsSegment object with helper methods to analyze the pairs line
        """
        # Strip the line and break into whitespace-delimited fields
        stripped = line.strip()
        if not stripped:
            raise StopIteration
        fields = stripped.split()

        # Map the values ("fields") to the column names ("self.indexed_columns")
        field_vals = {self.indexed_columns[idx]: val for idx, val in enumerate(fields)}

        return PairsSegment(**field_vals)

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
