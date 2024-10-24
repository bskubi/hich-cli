from hich.cli.paramlist import ParamList

# See tests/test_cli_types.py for unit tests

class _StrList(ParamList):
    name = "str_list"

    def value_type(self):
        return str

    def to_type(self, value):
        try:
            return str(value)
        except:
            raise ValueError(f"{value} is not a valid string")

StrList = _StrList()