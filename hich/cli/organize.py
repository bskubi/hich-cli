import click

@click.command
@click.option("--format", "--fmt", "fmt",
    type = click.Choice(['fasta', 'fastq', 'seqio', 'sam', 'bam', 'sambam', 'alignment', 'pairs']), required = True,
    help = "Alignment data format")
@click.option("--f1", "-f", "--file", "--file1", type = str, required = True,
    help = "Path to first (or only) input sequencing data file")
@click.option("--f2", "--file2", "f2", default = None, type = str, show_default = True,
    help = "Path to second input sequencing data file")
@click.option("--out-dir", "--dir", "--output-dir", type = str, default = "",
    help = "Output directory")
@click.option("--annot-file", "--annot", "-a", "--annotations", default = None, type = str,
    help = ("Path to annotation file, a columnar text file mapping data file "
            "information such as a cell barcode to new information such as "
            "an experimental condition or cell ID. Annotation files with headers "
            "convert to a dict with format {col1_row: {col2:col2_row, col3:col3_row...}}."))
@click.option("--annot-has-header", "-h", type = bool, default = False, show_default = True,
    help = "Whether or not annotation file has a header row.")
@click.option("--annot-separator", "-s", type = str, default = "\t", show_default = repr('\t'),
    help = "The column separator character")
@click.option("--head", "--n_records", type = int, default = None, show_default = True,
    help = "Take only the first n records from the data files. Takes all records if not provided.")
@click.option("--key-code", "--kc", type = str, default = None,
    help = "Python code to extract an annotation row key from the current record.")
@click.option("--record-code", "--rc", type = str, default = None,
    help = "Python code to modify the record")
@click.option("--output-code", "--fc", "--filename", type = str, default = None,
    help = "Python code to select the record output.")
def organize(fmt,
             f1,
             f2,
             out_dir,
             annot_file,
             annot_has_header,
             annot_separator,
             head,
             key_code,
             record_code,
             output_code):
    """Reannotate and split sequencing data files


    """
    raise NotImplementedError("Hich organize is not implemented yet")