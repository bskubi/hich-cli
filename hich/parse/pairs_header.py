from pathlib import Path
from parse import parse

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

class PairsHeader:
    version_prefix = "## pairs format v"

    def __init__(self, text = "", chromsizes = {}, columns = [], command = []):
        self.text = text
        self.chromsizes = chromsizes
        self.columns = columns
        self.command = command

    def to_dict(self):
        return self.__dict__
    
    def to_string(self):
        return self.text

    def set_columns(self, columns: list[str]):
        new_columns_line = "#columns: " + " ".join(columns) + "\n"
        self.columns = columns
        header = parse("{start}#columns: {columns}\n{end}", self.text) or parse("{start}#columns: {columns}", self.text)
        start = header["start"]
        end = header["end"] if "end" in header else ""
        self.text = start + new_columns_line + end

    @classmethod
    def from_text(self, from_text):
        header = PairsHeader()
        header.text = from_text
        lines = header.text.split("\n")
        assert lines[0].startswith(PairsHeader.version_prefix), f"Pairs must start with ## pairs format v1.0 but first line was {line}"
        header.pairs_format_version = lines[0].removeprefix("## pairs format v")
        for line in lines[1:]:
            if not line.startswith("#"):
                break
            fields = line.split()
            field_type = fields[0]
            rest = line.removeprefix(field_type).lstrip()
            if field_type == "#chromsize:":
                contig, size = fields[1:]
                header.chromsizes[contig] = size
            elif field_type == "#command:":
                header.command.append(rest)
            elif field_type == "#columns:":
                header.columns = fields[1:]
            elif field_type.endswith(":"):
                field_name = field_type[1:-1]
                if field_name not in self.__dict__:
                    setattr(header, field_name, rest)
                elif isinstance(header.__dict__[field_name], str):
                    header.__dict__[field_name] = [header.__dict__[field_name], rest]
                else:
                    header.__dict__[field_name].append(rest)
        return header
    
    def __repr__(self):
        return repr(str(self))

    def __str__(self):
        return self.to_string()