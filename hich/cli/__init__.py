import click
from abc import ABC, abstractmethod
from pathlib import Path

class _ParamList(click.ParamType, ABC):

    def convert(self, value, param, ctx):
        try:
            result = [value] if isinstance(value, self.value_type()) else [self.to_type(item) for item in value.split(self.separator())]
            result = [r for r in result if r is not None]
            return result
        except ValueError as e:
            self.fail(f"Invalid {self.value_type()} value in list: {e}")
    
    def separator(self):
        return ","

    @abstractmethod
    def value_type(self):
        pass

    @abstractmethod
    def to_type(self, value):
        pass
    
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

class _IntList(_ParamList):
    name = "int_list"

    def value_type(self):
        return int

    def to_type(self, value):
        try:
            return int(value) if value else None
        except:
            raise ValueError(f"'{value}' is not a valid integer")

class _PathList(_ParamList):
    name = "path_list"

    def value_type(self):
        return Path

    def to_type(self, value):
        try:
            return Path(value)
        except:
            raise ValueError(f"{value} is not a valid path")

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

BooleanList = _BooleanList()
IntList = _IntList()
PathList = _PathList()
StrList = _StrList()