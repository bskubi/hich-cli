from smart_open import smart_open
from typing import Union
import click
import io
import pandas as pd
import polars as pl
import smart_open
import smart_open_with_pbgzip
import sys
import warnings

def read_pairs(read_from, 
batch_size: int = 10000, 
yield_columns_line: bool = True, 
exception_on_4dn_violation: bool = True) -> Union[str, pl.DataFrame]:
    """
    Generator that reads 4DN .pairs format in batches.
    https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md

    The first yielded item is the header as a string. If yield_columns_line is True,
    then the complete header will be returned. Otherwise, any lines starting with
    #columns: will be excluded from the header. This is to facilitate changing
    the header if the pairs table is to be reshaped.

    Subsequent yielded items are Polars DataFrames containing up to
    batch_size .pairs records. The fields will be named according to the
    #columns: line, whether or not yield_columns_line is True or False. All
    types will be strings.
    """
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    # Used to accumulate header lines or records for a batch
    header_lines = []
    records = []
    columns = None

    # Iterate through all the lines in the iterable
    for i, line in enumerate(read_from):
        
        if exception_on_4dn_violation:
            # Optionally, raise an exception on 4DN spec violations

            # Enforce that file starts with '## pairs format v1.0'
            if header_lines and not header_lines[0].startswith("## pairs format v1.0"):
                raise Exception((
                    f"4DN spec violation found in {read_from}, line {i}: {line}\n"
                    f"first header line is {header_lines[0]} but must start "
                    "with '## pairs format v1.0'"
                ))
            if header_lines is None:
                # Enforce that all header lines come before the first data line
                if line.startswith("#"):
                    raise Exception(
                        (f"4DN spec violation found in {read_from}, line {i}: {line}\n"
                        "header line starting with '#' "
                        "appears on line {i}, after the first data line. All header "
                        "lines must come before data lines.")
                    )
        
        if line.startswith("#columns:"):
            # When a #columns: line is found, use the subsequent
            # whitespace-separated strings as column names, with String type
            # for the polars schema. Add the line to the header lines.
            columns = line.split()[1:]
            schema = {col: pl.String for col in columns}

            if yield_columns_line:
                # We give the option not to add #columns: lines into the header
                # because this lets the client conveniently reshape the yielded
                # DataFrames to have a different schema and write the #columns:
                # line based on the result.
                header_lines.append(line)
        elif line.startswith("#"):
            # Add other header lines
            header_lines.append(line)
        else:
            if exception_on_4dn_violation and columns is None:
                # Enforce that there is a #columns: line somewhere in the
                # .pairs file header. Note that per the spec, the #columns:
                # line does NOT have to go immediately before the data entries.
                raise Exception((
                    f"4DN spec violation found in {read_from}, line {i}: {line}\n"
                    "No #columns: line found prior to first data line."
                ))

            # We are in the data entries now, so split the entries into individual
            # fields and accumulate in the growing batch.
            records.append(line.split())

            if header_lines is not None:
                # header_lines is a list prior to finding the first data entry.
                # If we enter this block, it means we just found the first
                # data entry, meaning we need to build the complete header
                # string and yield it. Then we set header_lines to None to
                # mark that we've exited the header region.
                header = "".join(header_lines)
                header_lines = None
                yield header
            elif len(records) == batch_size:
                # We have reached the target batch size, so build a dataframe
                # with the accumulated records and yield it as the latest batch.
                # Then reset the records to start accumulating another batch.
                result = pl.DataFrame(records, orient='row', schema=schema)
                records = []
                yield result
    if records:
        # If we have accumulated a partial batch when we run out of records,
        # yield them in a final partial batch.
        yield pl.DataFrame(records, orient='row', schema=schema)

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi
class PairsParser:
    def __init__(self, filename):
        self.filename = filename
        self.write_file = None
        
    def columns_row(self):
        """Returns the 0-indexed offset of the final header row with the column names

        row_index: 0-indexed offset of the final header row (header rows start with '#')
        columns_dict: {colname:index} dict of which columns correspond to which column names

        If a row starting with '#columns:' is present it is assumed to be the final header row.
        If no '#columns:' row is present, returns the last header row with the minimal seven
        column names given by the .pairs specification.
        
        Returns: (row_index, columns_dict)
        """

        default_columns = ['readID', 'chrom1', 'pos1', 'chrom2', 'pos2', 'strand1', 'strand2']
        row = 0

        with smart_open.open(self.filename, "rt", encoding='utf-8') as file:
            for line in file:
                if line.startswith('#columns:'):
                    column_names = line.split()[1:]
                    return (row, column_names)
                elif line.startswith('#'):
                    row += 1
                else:
                    return (row-1, default_columns)
        return (row - 1, default_columns)

    def header(self):
        """Returns list of all lines starting with '#' until but not including the first line not starting with '#'
        """
        header = []
        with smart_open.open(self.filename, "rt", encoding="utf-8") as file:
            for line in file:
                if line.startswith('#'):
                    header.append(line)
                else:
                    break
        return header

    def batch_iter(self, n_rows):
        def read_chunk(skip_rows, columns):
            return pl.read_csv(self.filename,
                             skip_rows = skip_rows,
                             n_rows = n_rows,
                             raise_if_empty = False,
                             separator = '\t',
                             has_header = False,
                             new_columns = columns,
                             dtypes = {"readID":pl.String,
                                       "chrom1":pl.String,
                                       "chrom2":pl.String})
            
        columns_row, columns = self.columns_row()

        skip_rows = columns_row + 1
        with open(self.filename) as file:
            df = read_chunk(skip_rows, columns)
            skip_rows += len(df)
            yield df
            while len(df) == n_rows:
                df = read_chunk(skip_rows, columns)
                skip_rows += len(df)
                if not df.is_empty():
                    yield df

    def write_append(self, filename, df = None, header_end = None):
        warnings.filterwarnings("ignore", message="Polars found a filename")

        if self.write_file is None:
            self.write_file = smart_open.open(filename, "w")
            header_no_columns = self.header()[:-1]
            final_header_line_fields = header_no_columns[-1].split()
            
            if header_end:
                pn_field = [field[3:]
                    for field in final_header_line_fields
                    if field.startswith("PN:")]
                
                header_end.PP = pn_field[0] if pn_field else "null"
                
            header_no_columns = "".join(header_no_columns)
            columns = " ".join(["#columns:"] + df.columns) + "\n"
            header_end = str(header_end)
            header = header_no_columns + header_end + columns
            self.write_file.write(header)

        if df is not None:
            buffer = io.StringIO()
            df.write_csv(buffer,
                        include_header = False,
                        separator = '\t')
            buffer.seek(0)
            self.write_file.write(buffer.getvalue())
            buffer.close()

    def close(self):
        if self.write_file is not None:
            self.write_file.close()
            self.write_file = None