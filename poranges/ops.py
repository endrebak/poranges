from typing import List, Literal, Optional, Tuple

import polars as pl

from poranges.constants import ROW_NUMBER_PROPERTY
from poranges.four_quadrants_data import FourQuadrantsData
from poranges.groupby_join_result import GroupByJoinResult


def join(
    df: pl.LazyFrame,
    df2: pl.LazyFrame,
    suffix: str,
    starts: str,
    ends: str,
    starts_2: str,
    ends_2: str,
    by: Optional[List[str]] = None,
    closed_intervals: bool = False
) -> pl.LazyFrame:
    j = GroupByJoinResult(df, df2, starts, ends, starts_2, ends_2, suffix, by)
    if j.empty():
        return j.joined

    four_quadrants = FourQuadrantsData(
        j=j,
        closed_intervals=closed_intervals
    )

    return four_quadrants.overlapping_pairs()


def overlap(
        df: pl.LazyFrame,
        df2: pl.LazyFrame,
        starts: str,
        ends: str,
        starts_2: str,
        ends_2: str,
        by: Optional[List[str]] = None,
        closed_intervals: bool = False
) -> pl.LazyFrame:
    suffix = "_right__"
    j = GroupByJoinResult(df, df2, starts, ends, starts_2, ends_2, suffix, by)
    if j.empty():
        return j.joined

    four_quadrants = FourQuadrantsData(
        j=j,
        closed_intervals=closed_intervals
    )

    return four_quadrants.overlaps()


def closest(
        df: pl.LazyFrame,
        df2: pl.LazyFrame,
        *,
        starts: str,
        ends: str,
        starts_2: str,
        ends_2: str,
        direction: Literal["any", "left", "right"] = "any",
        include_overlapping: bool = True,
        suffix: str = "_right",
        k: int = 1,
        distance_col: Optional[str] = None,
        by: Optional[List[str]] = None
):
    _distance_col = "distance" if distance_col is None else distance_col

    j = _groupby_join(by, df, df2, ends, ends_2, starts, starts_2, suffix)

    if k > 0 and direction == "left":
        _closest = closest_nonoverlapping_left(
            df=df,
            df2=df2,
            starts=starts,
            ends_2=ends_2,
            suffix=suffix,
            k=k,
            distance_col=_distance_col
        )
    elif k > 0 and direction == "right":
        _closest = closest_nonoverlapping_right(
            df=df,
            df2=df2,
            ends=ends,
            starts_2=starts_2,
            suffix=suffix,
            k=k,
            distance_col=_distance_col
        )
    elif k > 0 and direction == "any":
        left = closest_nonoverlapping_left(
            df=df,
            df2=df2,
            starts=starts,
            ends_2=ends_2,
            suffix=suffix,
            k=k,
            distance_col=_distance_col
        )
        right = closest_nonoverlapping_right(
            df=df,
            df2=df2,
            ends=ends,
            starts_2=starts_2,
            suffix=suffix,
            k=k,
            distance_col=_distance_col
        )
        _closest = pl.concat([left, right])
    else:
        raise ValueError("`direction` must be one of 'left', 'right', or 'any'")

    if include_overlapping:
        overlaps = join(
            df=df,
            df2=df2,
            suffix=suffix,
            starts=starts,
            ends=ends,
            starts_2=starts_2,
            ends_2=ends_2,
            by=by
        ).with_columns(
                pl.lit(0).cast(pl.UInt64).alias(_distance_col)
        )
        _k_closest = pl.concat([overlaps, _closest]).sort(_distance_col).groupby(df.columns).agg(
            pl.all().head(k)
        ).explode(pl.exclude(df.columns))
        if distance_col is None:
            _k_closest = _k_closest.drop(_distance_col)
    else:
        _k_closest = _closest
    return _k_closest


def closest_nonoverlapping_left(
        df: pl.LazyFrame,
        df2: pl.LazyFrame,
        *,
        starts: str,
        ends_2: str,
        suffix: str = "_right",
        k: int = 1,
        distance_col: Optional[str] = None
):
    sorted_collapsed = df.select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(ends_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross", suffix=suffix)

    df_2_column_names_after_join = j.columns[len(df.columns) :]
    ends_2_renamed = df_2_column_names_after_join[df2.columns.index(ends_2)]

    diffs = "lengths_2in1"
    closest_endidx = "left_closest_endidx"
    closest_startidx = "left_closest_startidx"
    res = (
        j.select(
            [
                pl.all(),
                search(ends_2_renamed, starts, side="right")
                .cast(pl.Int64)
                .implode()
                .alias(closest_endidx),
            ]
        )
        .with_columns(
            pl.max([pl.col(closest_endidx).explode() - k, pl.lit(0)])
            .implode()
            .alias(closest_startidx)
        )
        .select(
            [
                pl.all(),
                add_length(
                    starts=closest_startidx,
                    ends=closest_endidx,
                    alias=diffs
                )
            ]
        )
        .select(
            [
                pl.all(),
                arange_multi(
                    starts=pl.col(closest_startidx).explode().filter(pl.col(diffs).explode().gt(0)),
                    diffs=pl.col(diffs).explode().filter(pl.col(diffs).explode().gt(0))
                ).alias("arange").implode()
            ]
        ).select(
            [
                pl.col(df.columns).explode().repeat_by(pl.col(diffs).explode()).explode().drop_nulls(),
                pl.col(df_2_column_names_after_join)
                .explode()
                .take(pl.col("arange").explode().cast(pl.UInt32))
            ]
        )
    )

    if distance_col is not None:
        res = res.with_columns(
            pl.col(starts).sub(pl.col(ends_2_renamed).explode()).cast(pl.UInt64).alias(distance_col)
        )

    return res


def closest_nonoverlapping_right(
        df: pl.LazyFrame,
        df2: pl.LazyFrame,
        *,
        ends: str,
        starts_2: str,
        suffix: str = "_right",
        k: int = 1,
        distance_col: Optional[str] = None,
        by: Optional[List[str]] = None
):
    sorted_collapsed = df.select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(starts_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross", suffix=suffix, by=by)

    df_2_column_names_after_join = j.columns[len(df.columns) :]
    starts_2_renamed = df_2_column_names_after_join[df2.columns.index(starts_2)]

    diffs = "lengths_2in1"
    right_closest_startidx = "right_closest_startidx"
    right_closest_endidx = "right_closest_endidx"
    res = (
        j.select(
            [
                pl.all(),
                search(starts_2_renamed, ends, side="left")
                .cast(pl.Int64)
                .implode()
                .alias(right_closest_startidx),
            ]
        )
        .with_columns(
            pl.min([pl.col(right_closest_startidx).explode() + k, pl.col(starts_2_renamed).explode().len()])
            .implode()
            .alias(right_closest_endidx)
        )
        .select(
            [
                pl.all(),
                add_length(
                    starts=right_closest_startidx,
                    ends=right_closest_endidx,
                    alias=diffs
                )
            ]
        )
        .select(
            [
                pl.all(),
                arange_multi(
                    starts=pl.col(right_closest_startidx).explode(),
                    diffs=pl.col(diffs).explode(),
                ).alias("arange").implode()
            ]
        )
    ).select(
        pl.col(df.columns).explode().repeat_by(pl.col(diffs).explode()).explode().drop_nulls(),
        pl.col(df_2_column_names_after_join).explode().take(pl.col("arange").explode().cast(pl.UInt32))
    )

    if distance_col is not None:
        res = res.with_columns(
            pl.col(starts_2_renamed).sub(pl.col(ends).explode()).cast(pl.UInt64).add(pl.lit(1)).alias(distance_col)
        )

    return res


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
