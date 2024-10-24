import click
from abc import ABC, abstractmethod
from typing import List

# See tests/test_cli_types.py for unit tests

class ParamList(click.ParamType, ABC):

    def __init__(self, separator = ",", do_strip = True, chars_to_strip = None):
        assert separator, f"In {str(self)}, separator was '{separator}' which will be nonfunctional. Common functional separators include delimiters like ',', ' ', ';', etc."
        self.separator = separator
        self.do_strip = do_strip
        self.chars_to_strip = chars_to_strip

    def convert(self, value, param, ctx):
        try:
            if not isinstance(value, str) and isinstance(value, self.value_type()):
                result = [value]
            else:
                result = []
                split = self.split(value)
                for item in split:
                    stripped = self.strip(item)
                    casted = self.to_type(stripped)
                    result.append(casted)

            result = [r for r in result if r is not None]
            return result
        except ValueError as e:
            self.fail(f"Invalid {self.value_type()} value in argument {param} with value {value}: {e}")
        except TypeError as e:
            self.fail(f"Using {str(self)} to parse argument '{param}' with value '{value}' ({type(value)}) resulted in error: {e}")

    def split(self, s: str) -> List[str]:
        "Split on my separator and drop empty strings from the split".strip()
        return [it for it in s.split(self.separator) if it]

    def strip(self, s: str) -> str:
        "Strip specific characters from string"
        if self.do_strip:
            return s.strip(self.chars_to_strip) if self.chars_to_strip else s.strip()
        return s

    @abstractmethod
    def value_type(self):
        pass

    @abstractmethod
    def to_type(self, value):
        pass

    def __str__(self):
        return f"{type(self)}: separator: '{self.separator}' ({type(self.separator)}), strip = '{self.do_strip}' ({type(self.do_strip)}), strip_chars: '{self.chars_to_strip}' ({type(self.chars_to_strip)})"
    