import click
from pathlib import Path
from tinydb import TinyDB, Query
from json import loads
import datetime
import os
import pandas as pd
import sys

@click.command
@click.option('--report', '-r', type = click.Path(), default = 'hich_all_output.html', show_default=True, help = 'Path of output HTML report')
@click.option('--db', '-d', type = click.Path(), default = 'hich_all_output.json', show_default=True, help = 'Path of output JSON database')
@click.option('--gather', '-g', type = str, default = 'hich_output.json', show_default = True, help = 'Glob expression for filename to gather')
@click.option("--work", '-w', type = click.Path(), default = ".", show_default = True, help = 'Root of work directory to gather from')
@click.option('--if-exists', '-e', default = 'use-unchanged', type = click.Choice(['use-unchanged', 'overwrite', 'append'], case_sensitive = False), help = 'Behavior when database exists.')
@click.option('--silent', '-s', is_flag = True, default = False, help = "Report messages on conflicts and errors")
def gather(report, db, gather, work, if_exists, silent):
    def convert_to_absolute_paths(data, work: Path) -> dict:
        if isinstance(data, dict):
            # If data is a dictionary, apply function to each value
            return {key: convert_to_absolute_paths(value, work) for key, value in data.items()}
        elif isinstance(data, list):
            # If data is a list, apply function to each item in the list
            return [convert_to_absolute_paths(item, work) for item in data]
        elif isinstance(data, str):
            # If data is a string and it represents an existing path, convert it to absolute path
            path = work / data
            if path.exists():
                return str(path.resolve())
            else:
                return data

        return data
    
    db_path = Path(db)
    if db_path.exists() and if_exists == 'overwrite':
        db_path.unlink()
        if not silent:
            print(f"Overwriting database at {db_path}", file = sys.stderr)

    db = TinyDB(db_path)

    if (db_path.exists() and (if_exists == 'append' or if_exists == 'overwrite')) or not db_path.exists():
        for path in Path(work).rglob(gather):
            with open(path) as file:
                record = loads(file.read())
            date_modified = datetime.datetime.fromtimestamp(path.stat().st_mtime)
            metadata = {'metadata': {'stage_dir': str(path.parent.resolve()), 'modified': str(date_modified)}}
            record = convert_to_absolute_paths(record, path.parent)
            record.update(metadata)
            db.insert(record)
    elif if_exists == 'use-unchanged' and not silent:
        print(f"Using original database at {db_path}", file = sys.stderr)
    
    records = sorted(db.all(), key = lambda x: x['metadata']['modified'])
    df = pd.json_normalize(records)
    df.to_html(report)


    