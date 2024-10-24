from pydantic import BaseModel, conlist, field_validator
from typing import Set, List
from collections import Counter
from hich.parse.pairs_schema import PairsSchema
from typing import Union
import warnings

class PairsColumns(BaseModel):
    columns: conlist(str, min_length=4)
    
    @field_validator('columns')
    def has_required_columns(cls, column_names: List[str]):
        unofficial = [name for name in column_names if (name in PairsSchema.common_unofficial_colnames())]
        if unofficial:
            recommended = [PairsSchema.make_official(name) for name in unofficial]
            warnings.warn(f"Attempted to create PairsColumns with common unofficial column names: {unofficial}; consider using {recommended}", UserWarning)
        
        column_names = [PairsSchema.make_official(name) for name in column_names]

        required_columns = {"chr1", "pos1", "chr2", "pos2"}
        if not required_columns.issubset(column_names):
            missing = required_columns - set(column_names)
            raise ValueError(f"Pairs file missing required columns: {', '.join(missing)}, check '#columns:' line")
        return column_names
    
    @field_validator('columns')
    def all_column_names_unique(cls, column_names: List[str]):
        column_name_counts = Counter(column_names)
        duplicates = set([name for name in column_name_counts if column_name_counts[name] > 1])
        if duplicates:
            raise ValueError(f"Attempted to create PairsColumns with duplicate column names: {duplicates}")
        return column_names

    @classmethod
    def is_columns_line(cls, line: str) -> bool:
        return line.startswith("#columns:")

    @classmethod
    def from_columns_line(cls, line: str) -> "PairsColumns":
        def crash_value_error(explain: str):
            preamble = f"Tried to create PairsColumns from '{line}', but it could not be parsed"
            raise ValueError(f"{preamble} {explain}".strip())
        
        if not PairsColumns.is_columns_line(line):
            crash_value_error("because it does not start with '#columns:'")

        fields = line.split()

        if len(fields) < 2:
            crash_value_error("because it did not contain any fields, just a '#columns:' prefix")
        
        column_names = fields[1:]
        official_column_names = [PairsSchema.make_official(name) for name in column_names]

        return PairsColumns(columns = official_column_names)

    def index(self, column: str) -> int:
        official_name = PairsSchema.make_official(column)
        return columns.index(official_name)
    
    def __getitem__(self, get: Union[int, str]) -> str:
        if isinstance(get, str):
            return self.index(get)
        return self.columns[index]
    
    def __setitem__(self, index: int, column_name: str) -> None:
        self.columns[index] = PairsSchema.make_official(column_name)
    
    def __eq__(self, other: Union[List[str], "PairsColumns"]) -> bool:
        if hasattr(other, "columns"):
            return self.columns == other.columns
        else:
            return self.columns == other