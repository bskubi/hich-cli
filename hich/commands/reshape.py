import click
import smart_open_with_pbgzip
from smart_open import smart_open
from hich.pairs import read_pairs
import polars as pl
import sys
import io

@click.command()
@click.option("--read_from", type=str, default="")
@click.option("--output_to", type=str, default="")
@click.option("--parse", type=str, nargs = 3, multiple=True, default=[], help="Format is --update [FROM_COL] [TO_COL] '[PATTERN]' as in --pattern 'readID' 'cellID' '{cellID}:{ignore}'")
@click.option("--placeholder", type=str, nargs = 2, multiple=True, default=[], help="Format is --placeholder [COL] [PLACEHOLDER] which replaces every column value with the placeholder string")
@click.option("--regex", type=str, nargs = 4, multiple=True, default=[], help="Format is --placeholder [FROM_COL] [TO_COL] [REGEX] [GROUP_INDEX] which extracts the group index specified (0=whole pattern) from the given regex from FROM_COL and sets it as the value in TO_COL")
@click.option("--drop", type=str, multiple=True, default=[], help="Column to drop")
@click.option("--select", type=str, default = "", help="Space-separated list of output column names to output in the order specified")
@click.option("--batch-size", type=int, default=10000, help="Number of records per batch")
def reshape(read_from, output_to, parse, placeholder, regex, drop, select, batch_size):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    read_from = smart_open(read_from, "rt") if read_from else sys.stdin
    reader = read_pairs(read_from, yield_columns_line=False, batch_size=batch_size)
    header = next(reader)
    output = smart_open(output_to, "wt") if output_to else None
    parse_cols = [
        (pl.col(from_col)
           .map_elements(lambda x: _parse(pattern, x)[to_col], return_dtype=pl.String)
           .alias(to_col))
        for from_col, to_col, pattern in parse
    ]
    regex_cols = [
        pl.col(from_col).str.extract(pattern, int(group_index)).alias(to_col)
        for from_col, to_col, pattern, group_index in regex
    ]
    placeholder_cols = [
        pl.lit(lit).alias(col)
        for col, lit in placeholder
    ]
    update_cols = parse_cols + regex_cols + placeholder_cols
    select = select.split()
    
    header_written = False

    if output:
        output.write(header)
    else:
        click.echo(header, nl=False)
    for df in reader:
        df = df.with_columns(*update_cols) if update_cols else df
        df = df.drop(drop) if drop else df
        df = df.select(select) if select else df
        if output:
            if not header_written:
                output.write("#columns: " + " ".join(df.columns) + "\n")
                header_written = True
            df = df.to_pandas()
            df.to_csv(output, sep="\t", header=False, index=False)
        else:
            df = df.to_pandas()
            buffer = io.StringIO()
            if not header_written:
                click.echo("#columns: " + " ".join(df.columns) + "\n", nl=False)
                header_written = True

            df.to_csv(buffer, sep="\t", header=False, index=False)
            click.echo(buffer.getvalue(), nl=False)