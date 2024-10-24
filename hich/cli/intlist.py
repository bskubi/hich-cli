from hich.cli.paramlist import ParamList

# See tests/test_cli_types.py for unit tests

class _IntList(ParamList):
    name = "int_list"

    def value_type(self):
        return int

    def to_type(self, value):
        try:
            return int(value) if value else None
        except:
            raise ValueError(f"'{value}' is not a valid integer")

IntList = _IntList()