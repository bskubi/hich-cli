import polars as pl

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

class BedpePairs:
    def __init__(self, df):
        self.df = df
    
    def fragtag(self, frag_index):
        """Map .pairs end pos to restriction fragment index, start, end
        
        This algo centers around numpy searchsorted which will "Find indices
        where elements should be inserted to maintain order." We partition each
        end by chromosome, sort by position, then apply np.searchsorted on
        the sorted fragment index ends. This gives the fragment each end
        maps to, and from this we can extract the start and end position of the
        fragment.

        This mapping depends on partitioning each pair by chromosome, then by
        position. To make it possible to restore order, we start by assigning
        a row index to the pairs, which is extracted along with the chrom and
        pos for each end of the pair.
        """

        # Assign pair id to restore order after mapping pair ends.
        pair_id = "__pairID__"
        pair_id_df = self.df.with_row_index(pair_id)
        

        for end in ["1", "2"]:
            # Get end-specific column names
            chrom_col = f"chrom{end}"
            pos_col = f"pos{end}"
            frag_colnames = [f"rfrag{end}", f"rfrag_start{end}", f"rfrag_end{end}"]

            # Select the pair id, chromosome, and position, then split into a 
            # {chromosome : dataframe} dictionary to facilitate per-chromosome
            # fragment mapping.
            end_chroms = pair_id_df.select(pair_id, chrom_col, pos_col) \
                                   .partition_by([chrom_col], as_dict = True)

            for chrom, chrom_df in end_chroms.items():
                # Iterate through the chromosomes and chrom- and pair end-
                # specific dataframes.

                # Ensure dataframe is sorted by position to allow searchsorted
                # Then extract positions and map them to restriction fragments
                chrom_df = chrom_df.sort(by = [pos_col])
                positions = chrom_df[pos_col].to_list()
                frag_cols = BedpePairs.frag_columns(frag_index,
                                                    chrom,
                                                    positions,
                                                    frag_colnames)
                
                # Add frag columns to the dataframe subset used for mapping.
                # As we'll be recovering the original row order by sorting by
                # pair ID and the chrom and pos are preserved in the original
                # dataframe, we drop the chrom and pos from the fragments
                # dataframe. Then we extract just the fragment column names
                # once they are sorted by pair ID and attach them to the 
                # complete per-chromosome dataframe.
                chrom_df = chrom_df.with_columns(frag_cols)
                chrom_df = chrom_df.drop([chrom_col, pos_col])
                frag_cols = chrom_df.sort(by = [pair_id]) \
                                    .select(frag_colnames)                              
                end_chroms[chrom] = end_chroms[chrom].with_columns(frag_cols)
            
            # Reconcatenate all the per-chrom dataframes with the fragment
            # columns and drop (I believe the drop is redundant but haven't
            # checked.)
            end_frags = pl.concat(end_chroms.values()) \
                          .drop([chrom_col, pos_col])

            # Join on the pair id
            pair_id_df = pair_id_df.join(end_frags,
                                         on = [pair_id],
                                         how = "inner",
                                         coalesce = True)
        # Resort by the original pair id to restore the original order, then
        # drop the pair id column to get the original dataframe with the
        # fragment columns for end 1 and end 2 as the final 6 columns.
        pair_id_df = pair_id_df.sort(by = [pair_id])
        return pair_id_df.drop(pair_id)

    @classmethod
    def frag_columns(self,
                         frag_index,
                         chrom,
                         positions,
                         frag_colnames):
        if chrom not in frag_index:
            count = len(positions)
            rfrag = pl.repeat(-1, count)
            rfrag_start = pl.repeat(0, count)
            rfrag_end = pl.repeat(0, count)
        else:
            # Get the indices where the positions sort to in the restriction fragments
            # for the chromosome -- this is the main event that identifies the restriction fragments
            rfrag_indices = frag_index.search(chrom, positions)

            # Create column for the restriction fragment index and for the start and end positions
            rfrag = pl.Series(rfrag_indices)

            # Create column for start position of the restriction fragment
            # Adjust exact positioning to match pairtools output
            # (Set 0 to -1, then add 1 to all)
            rfrag_start = frag_index.starts(chrom) \
                            .gather(rfrag_indices) \
                            .replace(0, -1) \
                            + 1

            # Create column for end position of the restriction fragment
            # Adjust exact position to match pairtools output (add 1)
            rfrag_end = frag_index.ends(chrom) \
                        .gather(rfrag_indices) \
                        + 1
        
        # Cast and set the name of the fragment columns
        frag_cols = [rfrag, rfrag_start, rfrag_end]
        for i, frag_col in enumerate(frag_cols):
            frag_cols[i] = frag_col.cast(pl.Int64).alias(frag_colnames[i])

        return frag_cols