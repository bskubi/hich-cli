import click
from abc import ABC, abstractmethod

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
    