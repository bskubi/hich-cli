from hich.cli.util.paramlist import _ParamList
from pathlib import Path

class _PathList(_ParamList):
    name = "path_list"

    def value_type(self):
        return Path

    def to_type(self, value):
        try:
            return Path(value)
        except:
            raise ValueError(f"{value} is not a valid path")

PathList = _PathList()