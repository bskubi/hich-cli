import click
import sys
from pathlib import Path
import polars as pl
import pandas as pd
from typing import NewType, Union, IO

ReadableToDF = NewType('ReadableToDF', Union[str, Path, IO[str], IO[bytes], bytes])
WriteableToDF = NewType('WriteableToDF', Union[str, Path, IO[str], IO[bytes], None])

def df_to_disk_or_stdout(df, output: Union[WriteableToDF, None], *args, **kwargs):
    "Write a DataFrame to csv or stdout if output_path is None"
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    output_file = output or sys.stdout
    if isinstance(df, pd.DataFrame):
        to_stdout = df.to_csv(output_file, *args, **kwargs)
    elif isinstance(df, pl.DataFrame):
        to_stdout = df.write_csv(output_file, *args, **kwargs)
    else:
        TypeError(f"df_to_disk_or_stdout expects polars.DataFrame or pandas.DataFrame, got {type(df)}")
    if isinstance(to_stdout, str):
        click.echo(to_stdout)