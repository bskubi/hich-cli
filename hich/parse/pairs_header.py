from pathlib import Path
from parse import parse
from typing import List, Dict

class PairsHeader:
    """Represents the #-prefixed header of a 4DN .pairs file

    .pairs file format spec:
    https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md

    After parsing header text, the PairsHeader will have the following attributes:
        chromsizes: a {chromname (str): size (int)} dict
        columns: a list of string names of the columns of the data section of the pairs file
        command: a list of lines prefixed by #command:
        It may also have additional attributes that are strings or lists of strings
        for any other lines prefixed by "#[field_name]:"
        These will be accessible under the PairsHeader attribute named field_name, like
        pairs_header.custom_field
    """

    # Mandatory first line of .pairs file per 4DN spec
    version_prefix = "## pairs format v"

    def __init__(
            self, 
            text: str = "", 
            chromsizes: Dict[str, int] = {}, 
            columns: List[str] = [], 
            command: List[str] = []
            ):
        # text is the raw text of the header
        self.text: str = text

        # chromsizes maps the chrom names (key) to size in bp (val)
        self.chromsizes: Dict[str, int] = chromsizes

        # list of column names for the data section of the .pairs file
        self.columns: List[str] = columns

        # stores the command section of the header
        self.command: List[str] = command


    def to_dict(self) -> Dict:
        """Convert self to a dictionary

        Returns:
            Dict: converts self to a dict
        """
        return self.__dict__
    
    def to_string(self) -> str:
        """Return raw text of the header

        Returns:
            str: Raw text of the header
        """
        return self.text

    def set_columns(self, columns: List[str]) -> None:
        """Set the columns of the header

        Args:
            columns (list[str]): The list of columns to set
        """
        # Create a new columns line (the final line of the header)
        new_columns_line = "#columns: " + " ".join(columns) + "\n"

        # Store the new columns line
        self.columns = columns

        # Extract the header prior to the columns line, the columns, and any text subsequent to the columns
        # from the raw header text. If the file contains only a header with no newline and end matter then
        # the first parse returns None and the second parses it.
        header = parse("{start}#columns: {columns}\n{end}", self.text) or parse("{start}#columns: {columns}", self.text)

        # Extract the part of the header prior to the start and the part still within the header but after the columns line
        start = header["start"]
        end = header["end"] if "end" in header else ""

        # Replace the original columns line with the new one in the raw header text
        self.text = start + new_columns_line + end

    @classmethod
    def from_text(self, from_text: str) -> "PairsHeader":
        """Parse the header fields out of raw header text

        Args:
            from_text (str): raw header text in 4DN format

        Returns:
            PairsHeader: A PairsHeader object parsed from the raw text
        """

        # Create the header object and set the raw text
        header = PairsHeader()
        header.text = from_text
        lines = header.text.split("\n")

        # Validate that we have the mandatory header
        assert lines[0].startswith(PairsHeader.version_prefix), f"Pairs must start with ## pairs format v1.0 but first line was {line}"

        # Provided the header exists, extract and store the file format version
        header.pairs_format_version = lines[0].removeprefix("## pairs format v")

        # Iterate through the rest of the header lines until the first line not prefixed by # is found (the first data line)
        for line in lines[1:]:
            if not line.startswith("#"):
                break

            # The fields of header lines are whitespace-delimited
            fields = line.split()

            # First field is the field type
            field_type = fields[0]

            # Remove the field type and keep the rest of the line
            rest = line.removeprefix(field_type).lstrip()

            # Handle reserved keywords for the 4DN spec to create the
            # chromsizes dict, command list, and columns list
            if field_type == "#chromsize:":
                # Lines like #chromsize [chromname] [size]
                contig, size = fields[1:]
                header.chromsizes[contig] = size
            elif field_type == "#command:":
                # Store as a command
                header.command.append(rest)
            elif field_type == "#columns:":
                # Store the columns fields
                header.columns = fields[1:]
            elif field_type.endswith(":"):
                # Store any other fields under custom names, dropping the leading # and trailing :
                # under self.[field_name]. The first time this field_name is found, store the value as
                # a string. The second time, turn it into a list with both values, and extend it with
                # any additional values found from that point on.
                field_name = field_type[1:-1]

                if field_name not in self.__dict__:
                    # If not already an attribute of the PairsHeader, make it one
                    setattr(header, field_name, rest)

                elif isinstance(header.__dict__[field_name], str):
                    # If it is a string attribute of the PairsHeader, turn it into a list of the values found
                    header.__dict__[field_name] = [header.__dict__[field_name], rest]
                else:
                    # If it is already a list in the PairsHeader, extend the list
                    header.__dict__[field_name].append(rest)
        
        return header
    
    def valid_header(self) -> bool:
        """Determines whether the minimum information required in the header is found

        Specifically requires that columns are specified and a version prefix is found.

        Returns:
            bool: Returns True if this is a validly formatted PairsHeader
        """
        return self.columns and self.version_prefix

    def __repr__(self):
        return repr(str(self))

    def __str__(self):
        return self.to_string()