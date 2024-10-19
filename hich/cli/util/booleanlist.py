from hich.cli.util.paramlist import _ParamList

class _BooleanList(_ParamList):
    name = "boolean_list"

    def value_type(self):
        return bool

    def to_type(self, value):
        if value.lower() in ('true', '1', 'yes'):
            return True
        elif value.lower() in ('false', '0', 'no'):
            return False
        else:
            raise ValueError(f"{value} is not a valid boolean")

BooleanList = _BooleanList()
