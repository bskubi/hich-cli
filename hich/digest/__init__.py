import click
from Bio import SeqIO
from Bio.Restriction import RestrictionBatch
from Bio.Seq import Seq
import sys
from itertools import chain
from smart_open import smart_open
import polars as pl

def sorted_unique_cut_sites(seq_record, enzymes, cutshift = 1):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    df = {"chrom":[], "start":[], "end":[]}
    digest = enzymes.search(seq_record.seq)
    digest = [digest[enzyme] for enzyme in digest]
    cut_sites = sorted(list(set(chain(*digest))))
    if cut_sites:
        cut_sites = (pl.Series(cut_sites) + cutshift).to_list()
    return cut_sites

def chrom_frags_df(seq_record, enzymes, cutshift = 1):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    start = [0]
    end = [len(seq_record.seq)]
    frag_ends = start + sorted_unique_cut_sites(seq_record, enzymes, cutshift) + end
    
    chrom = seq_record.id
    frag_count = len(frag_ends)-1

    chrom_col = [chrom]*frag_count
    start_col = frag_ends[:-1]
    end_col = frag_ends[1:]
    df = {"chrom":chrom_col, "start":start_col, "end":end_col}
    return pl.DataFrame(df)

def write_bed_file(frag_index, output_file):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    handle = smart_open(output_file, "w") if output_file else sys.stdout
    frag_index.write_csv(handle, include_header=False, separator="\t")

def make_frag_index(reference_filename,
                    enzyme_names,
                    output_file,
                    startshift = 0,
                    endshift = 0,
                    cutshift = 1):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    # Load the reference genome
    reference_file = smart_open(reference_filename, "rt")
    seq_record = SeqIO.parse(reference_file, "fasta")
    genome = SeqIO.to_dict(seq_record)

    enzymes = RestrictionBatch(enzyme_names)
    
    # Process each sequence record in the genome
    chrom_frag_indexes = []
    for seq_record in genome.values():
        chrom_frag_index = chrom_frags_df(seq_record, enzymes, cutshift)
        chrom_frag_indexes.append(chrom_frag_index)

    frag_index = pl.concat(chrom_frag_indexes)
    frag_index = frag_index.with_columns(pl.col('start') + startshift)
    frag_index = frag_index.with_columns(pl.col('end') + endshift)

    # Write to BED file
    write_bed_file(frag_index, output_file)

def kit_names_to_enzymes(digest):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    digest = set(digest)
    enzymes = set()

    # If the digest is a known kit name, map it to the corresponding enzymes
    kits = {
        ("Arima Genome-Wide HiC+", "Arima"): ["DpnII", "HinfI"],
        ("Phase Proximo 2021+ Plant", "Phase Plant"): ["DpnII"],
        ("Phase Proximo 2021+ Animal", "Phase Animal"): ["DpnII"],
        ("Phase Proximo 2021+ Microbiome", "Phase Microbiome"): ["Sau3AI", "MluCI"],
        ("Phase Proximo 2021+ Human", "Phase Human"): ["DpnII"],
        ("Phase Proximo 2021+ Fungal", "Phase Fungal"): ["DpnII"]
    }
    digest_removed = set()
    for kit, kit_enzymes in kits.items():
        for digest_item in digest:
            if digest_item in kit:
                for enzyme in kit_enzymes:
                    enzymes.add(enzyme)
                digest_removed.add(digest_item)
    
    digest = digest.difference(digest_removed)

    # Add any non-kit enzymes to the digest
    enzymes = enzymes.union(digest)

    print(enzymes)
    return enzymes

def make_fragment_index(output, startshift, endshift, cutshift, reference, digest):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    enzyme_names = kit_names_to_enzymes(digest)

    return make_frag_index(reference,
                    enzyme_names,
                    output,
                    startshift,
                    endshift,
                    cutshift)