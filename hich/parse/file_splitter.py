from abc import ABC, abstractmethod
from dataclasses import dataclass, field

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class FileSplitter:
    handles: dict = field(default_factory = dict[str, object])
    remove: str = ""
    
    @abstractmethod
    def access(self, filename: str): ...
    """Get a file handle ready for writing records"""

    @abstractmethod
    def write(self, filename: str, record: object): ...
    """Write a record"""

    def __del__(self):
        """Remove a given file after the FileSplitter is destroyed"""
        if self.remove and Path(self.remove).exists():
            Path(self.remove).unlink()