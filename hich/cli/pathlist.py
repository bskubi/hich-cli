from hich.cli.paramlist import ParamList
from pathlib import Path

# Not currently unit tested as of 2024/10/20 - Ben Skubi

class _PathList(ParamList):
    name = "path_list"

    def value_type(self):
        return Path

    def to_type(self, value):
        try:
            return Path(value)
        except:
            raise ValueError(f"{value} is not a valid path")

PathList = _PathList()