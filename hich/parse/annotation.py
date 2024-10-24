import polars as pl
"""
Annotation files are required to either have headers or must be given
headers at the command line.

A zero-offset row index will be included. It can be given a name by the user.
If it is not included, then any of the following names can be used to access it:
    "index", "idx", "Index"
These names only refer to index columns if it did not already exist in the annotation file.

A key column must be supplied. A particular annotation is accessed by extracting it
from information in the data file using annotations[key].index or annotations[key][index]

I think the way this should work is that we extract the key from the record object, obtaining
a value of 'key'

"""

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

class AnnotationFile(dict):

    def read_csv(self, file, key_col = None, **kwargs):
        df = pl.read_csv(file, **kwargs)
        key_col = key_col or df.columns[0]
        for i, row in enumerate(self.iter_rows(named = True)):
            key = row[key_col]
            self[key] = {"index": i, "annotations": row}
    
    def __getitem__(self, key):
        if key not in self:
            self.update({key: {"index": len(self)}})
        return self.get(key)
