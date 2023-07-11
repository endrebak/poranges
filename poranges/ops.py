from typing import List, Literal, Optional, Tuple

import polars as pl

from poranges.constants import ROW_NUMBER_PROPERTY


def add_length(starts: str, ends: str, alias: str) -> pl.Expr:
    return (
        pl.col(ends)
        .explode()
        .sub(pl.col(starts).explode())
        .alias(alias)
        .implode()
    )


def search(col1: str, col2: str, side: str = "left") -> pl.Expr:
    return pl.col(col1).explode().search_sorted(pl.col(col2).explode(), side=side)


def arange_multi(*, starts: pl.Expr, diffs: pl.Expr) -> pl.Expr:
    return (
        pl.int_ranges(
            start=starts,
            end=starts.add(diffs)
        ).explode().drop_nulls()
    )


def merge(
        df: pl.LazyFrame,
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
    by
        Columns to group by.
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
    if df.first().collect().shape[0] == 0:
        return df

    lazy_df = df.lazy().sort([starts, ends])

    if by is None:
        lazy_df = lazy_df.select(pl.all().implode()).with_row_count(ROW_NUMBER_PROPERTY).select(pl.all().explode())
        grpby_ks = [ROW_NUMBER_PROPERTY]
    else:
        grpby_ks = by

    ordered = (
        lazy_df.groupby(grpby_ks).agg(
            pl.all(),
            pl.col(ends).cummax().alias("max_ends")
        )
        .groupby(grpby_ks).agg(
            pl.all().explode(),
            _cluster_borders_expr(merge_bookended, min_distance, starts).alias("cluster_borders")
        )
        .groupby(grpby_ks).agg(
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
    cluster_frame = ordered.explode("cluster_starts", "cluster_ends", "take")

    rename = {
        "cluster_starts": starts,
        "cluster_ends": ends,
    }

    if keep_original_columns:
        cols_not_in_grpby_ks = [c for c in df.columns if c not in grpby_ks]
        cluster_frame = ordered.explode("cluster_starts", "cluster_ends", "take").groupby(grpby_ks).agg(
            pl.col("cluster_starts", "cluster_ends"),
            pl.col(cols_not_in_grpby_ks).explode(),
            pl.col("take").shift_and_fill(0).explode().alias("take_from"),
            pl.col("take").explode().alias("take_until"),
        ).explode(
            "cluster_starts", "cluster_ends", "take_from", "take_until"
        ).select(
            pl.col(grpby_ks),
            pl.col(["cluster_starts", "cluster_ends"]).explode(),
            pl.col(cols_not_in_grpby_ks).list.slice(
                pl.col("take_from"), pl.col("take_until").sub(pl.col("take_from"))
            )
        )
        rename.update(
            {
                starts: starts + suffix,
                ends: ends + suffix
            }
        )

    if by is None:
        cluster_frame = cluster_frame.drop(ROW_NUMBER_PROPERTY)

    if not keep_original_columns:
        cluster_frame = cluster_frame.select(by + ["cluster_starts", "cluster_ends"])

    return cluster_frame.rename(rename)


def _clusters(cluster_borders_expr, df, ends, starts) -> pl.Expr:
    return (
        df.sort([starts, ends]).select(
            pl.all().implode(),
            pl.col(ends).cummax().implode().alias("max_ends")
        )
        .with_columns(
            cluster_borders_expr.alias("cluster_borders").implode()
        )
        .select(
            pl.col(df.columns),
            pl.col("cluster_borders").explode().cumsum()
            .sub(1).slice(0, pl.col("cluster_borders").explode().len() - 1).implode().alias("cluster_ids"),
            pl.col(starts).explode().filter(
                pl.col("cluster_borders").explode().slice(0, pl.col("cluster_borders").explode().len() - 1),
            ).alias("cluster_starts").implode(),
            pl.col("max_ends").explode().filter(
                pl.col("cluster_borders").explode().slice(1, pl.col("cluster_borders").explode().len())
            ).alias("cluster_ends").implode(),
        )
    )


def _cluster_borders_expr(merge_bookended, min_distance, starts) -> pl.Expr:
    if not merge_bookended:
        cluster_borders_expr = pl.col(starts).explode().shift(-1).gt(
            pl.col("max_ends").explode().add(min_distance)).shift_and_fill(True, periods=1).extend_constant(True, 1)
    else:
        cluster_borders_expr = pl.col(starts).explode().shift(-1).ge(
            pl.col("max_ends").explode().add(min_distance)).shift_and_fill(True, periods=1).extend_constant(True, 1)
    return cluster_borders_expr
