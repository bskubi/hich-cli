from hich.cli.util.paramlist import _ParamList

class _StrList(_ParamList):
    name = "str_list"

    def convert(self, value, param, ctx):
        try:
            return [self.to_type(item) for item in value.split(self.separator())]
        except ValueError as e:
            self.fail(f"Invalid {self.value_type()} value in list: {e}")

    def value_type(self):
        return str

    def to_type(self, value):
        try:
            return str(value)
        except:
            raise ValueError(f"{value} is not a valid string")

StrList = _StrList()