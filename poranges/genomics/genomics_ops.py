from typing import Optional, List

import polars as pl

from poranges import ops
from poranges.ops import join, overlap, closest

def merge(
        df: pl.LazyFrame,
        chromosome: str,
        starts: str,
        ends: str,
        by: Optional[List[str]] = None,
        merge_bookended: bool = False,
        keep_original_columns: bool = True,
        min_distance: int = 0,
        suffix: str = "_before_merge"
):
    """
    Merge overlapping intervals in a DataFrame.

    Parameters
    ----------
    df
        DataFrame with interval columns.
    chromosome
        Name of the column with the chromosomes.
    starts
        Name of the column with the start coordinates.
    ends
        Name of the column with the end coordinates.
    merge_bookended
        If True, merge intervals that are adjacent but not overlapping.
    keep_original_columns
        If True, keep the original columns in the output.
    min_distance
        Minimum distance between intervals to merge.
    suffix
        Suffix to add to the original column names.

    Returns
    -------
    DataFrame
        DataFrame with merged intervals.
    """
    lazy_df = df.lazy()
    grpby_ks = [chromosome] if by is None else [chromosome] + by
    ordered = (
        lazy_df.sort([starts, ends]).groupby(grpby_ks).agg(
            pl.all(),
            pl.col(ends).cummax().alias("max_ends")
        )
        .groupby(grpby_ks).agg(
            pl.all().explode(),
            ops._cluster_borders_expr(merge_bookended, min_distance, starts).alias("cluster_borders")
        )
        .groupby(grpby_ks).agg(
            pl.col(chromosome).repeat_by(pl.col("starts").list.lengths()).alias(chromosome + "_repeated"),
            pl.col(starts).explode().filter(
                pl.col("cluster_borders").explode().slice(0, pl.col("cluster_borders").explode().len() - 1),
            ).alias("cluster_starts"),
            pl.col("max_ends").explode().filter(
                pl.col("cluster_borders").explode().slice(1, pl.col("cluster_borders").explode().len())
            ).alias("cluster_ends"),
            pl.col("cluster_borders").explode().cumsum()
            .sub(1).slice(0, pl.col("cluster_borders").explode().len() - 1).value_counts(sort=True, multithreaded=False)
            .struct.field("counts").alias("cluster_ids").cumsum()
            .alias("take"),
            pl.exclude(grpby_ks).explode(),
            )
    )

    cluster_frame = ordered.select(
        pl.col([chromosome, "cluster_starts", "cluster_ends"])
    ).sort(chromosome)

    rename = {
        "cluster_starts": starts,
        "cluster_ends": ends,
    }

    if keep_original_columns:
        cols_not_in_grpby_ks = [c for c in df.columns if c not in grpby_ks]
        cols_to_explode = ["cluster_starts", "cluster_ends"] + cols_not_in_grpby_ks
        cluster_frame = ordered.explode("cluster_starts", "cluster_ends", "take").groupby(grpby_ks).agg(
            pl.col("cluster_starts", "cluster_ends"),
            pl.col(cols_not_in_grpby_ks).list.slice(
                pl.col("take").shift_and_fill(0), pl.col("take")
            )
        ).explode(cols_to_explode)
        rename.update(
            {
                starts: starts + suffix,
                ends: ends + suffix
            }
        )

    return cluster_frame.rename(rename)
