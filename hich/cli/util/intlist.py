from hich.cli.util.paramlist import _ParamList

class _IntList(_ParamList):
    name = "int_list"

    def value_type(self):
        return int

    def to_type(self, value):
        try:
            return int(value) if value else None
        except:
            raise ValueError(f"'{value}' is not a valid integer")

IntList = _IntList()