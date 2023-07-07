from typing import List, Literal, Optional

import polars as pl

STARTS_PROPERTY = "starts"
ENDS_PROPERTY = "ends"
STARTS2_PROPERTY = "starts_2"
ENDS2_PROPERTY = "ends_2"

STARTS_2IN1_PROPERTY = "starts_2in1"
ENDS_2IN1_PROPERTY = "ends_2in1"
STARTS_1IN2_PROPERTY = "starts_1in2"
ENDS_1IN2_PROPERTY = "ends_1in2"
MASK_1IN2_PROPERTY = "mask_1in2"
MASK_2IN1_PROPERTY = "mask_2in1"
LENGTHS_2IN1_PROPERTY = "lengths_2in1"
LENGTHS_1IN2_PROPERTY = "lengths_1in2"


def search(col1: str, col2: str, side: Literal["any", "right", "left"] = "left") -> pl.Expr:
    return pl.col(col1).explode().search_sorted(pl.col(col2).explode(), side=side)


def lengths(starts: str, ends: str, outname: str = "") -> pl.Expr:
    return pl.col(ends).explode().sub(pl.col(starts).explode()).explode().alias(outname)


def find_starts_in_ends(
    starts, ends, starts_2, ends_2, closed: bool = False
) -> List[pl.Expr]:
    side = "right" if closed else "left"

    return [
        search(starts_2, starts, side="left").alias(STARTS_2IN1_PROPERTY).implode(),
        search(starts_2, ends, side=side).alias(ENDS_2IN1_PROPERTY).implode(),
        search(starts, starts_2, side="right").alias(STARTS_1IN2_PROPERTY).implode(),
        search(starts, ends_2, side=side).alias(ENDS_1IN2_PROPERTY).implode(),
    ]


def compute_masks() -> List[pl.Expr]:
    return [
        pl.all(),
        pl.col(ENDS_2IN1_PROPERTY)
        .explode()
        .gt(pl.col(STARTS_2IN1_PROPERTY).explode())
        .implode()
        .alias(MASK_2IN1_PROPERTY),
        pl.col(ENDS_1IN2_PROPERTY)
        .explode()
        .gt(pl.col(STARTS_1IN2_PROPERTY).explode())
        .implode()
        .alias(MASK_1IN2_PROPERTY),
    ]


def apply_masks() -> List[pl.Expr]:
    return [
        pl.exclude(
            STARTS_1IN2_PROPERTY,
            STARTS_2IN1_PROPERTY,
            ENDS_1IN2_PROPERTY,
            ENDS_2IN1_PROPERTY,
        ),
        pl.col([STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY])
        .explode()
        .filter(pl.col(MASK_2IN1_PROPERTY).explode())
        .implode(),
        pl.col([STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY])
        .explode()
        .filter(pl.col(MASK_1IN2_PROPERTY).explode())
        .implode(),
    ]


def add_length(starts: str, ends: str, alias: str) -> pl.Expr:
    return (
        pl.col(ends)
        .explode()
        .sub(pl.col(starts).explode())
        .alias(alias)
        .implode()
    )


def add_lengths() -> List[pl.Expr]:
    return (
        [
            pl.all(),
            add_length(STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY, LENGTHS_2IN1_PROPERTY),
            add_length(STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY, LENGTHS_1IN2_PROPERTY)
        ]
    )


def repeat_frame(columns, startsin, endsin) -> pl.Expr:
    return (
        pl.col(columns)
        .explode()
        .repeat_by(pl.col(endsin).explode() - pl.col(startsin).explode())
        .explode()
    )


def mask_and_repeat_frame(columns, mask, startsin, endsin) -> pl.Expr:
    return (
        pl.col(columns)
        .explode()
        .filter(pl.col(mask).explode())
        .repeat_by(pl.col(endsin).explode() - pl.col(startsin).explode())
        .explode()
    )


def repeat_other(columns, starts, diffs):
    return (
        pl.col(columns)
        .explode()
        .take(
            pl.int_ranges(
                start=starts,
                end=starts.add(diffs)
            ).explode()
        )
    )


def arange_multi(*, starts: pl.Expr, diffs: pl.Expr) -> pl.Expr:
    return (
        pl.int_ranges(
            start=starts,
            end=starts.add(diffs)
        ).explode().drop_nulls()
    )


def join(
    df: pl.LazyFrame,
    df2: pl.LazyFrame,
    suffix: str,
    starts: str,
    ends: str,
    starts_2: str,
    ends_2: str,
) -> pl.LazyFrame:
    sorted_collapsed = df.sort(starts, ends).select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(starts_2, ends_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross", suffix=suffix)

    df_2_column_names_after_join = j.columns[len(df.columns) :]
    starts_2_renamed = df_2_column_names_after_join[df2.columns.index(starts_2)]
    ends_2_renamed = df_2_column_names_after_join[df2.columns.index(ends_2)]

    four_quadrants = (
        j.with_columns(
            find_starts_in_ends(starts, ends, starts_2_renamed, ends_2_renamed)
        )
        .with_columns(compute_masks())
        .with_columns(apply_masks())
        .with_columns(add_lengths())
    )
    # we use implode so we can use the cross product join; seemingly the only way to horizontally concat a lazy-frame
    top_left = four_quadrants.select(
        mask_and_repeat_frame(
            df.columns,
            MASK_2IN1_PROPERTY,
            STARTS_2IN1_PROPERTY,
            ENDS_2IN1_PROPERTY,
        )
    )
    bottom_left = four_quadrants.select(
        repeat_other(
            df.columns, pl.col(STARTS_1IN2_PROPERTY).explode(), pl.col(LENGTHS_1IN2_PROPERTY).explode()
        )
    )
    top_right = four_quadrants.select(
        repeat_other(
            df_2_column_names_after_join,
            pl.col(STARTS_2IN1_PROPERTY).explode(),
            pl.col(LENGTHS_2IN1_PROPERTY).explode(),
        )
    )
    bottom_right = four_quadrants.select(
        mask_and_repeat_frame(
            df_2_column_names_after_join,
            MASK_1IN2_PROPERTY,
            STARTS_1IN2_PROPERTY,
            ENDS_1IN2_PROPERTY,
        )
    )

    # we cannot horizontally concat a lazy-frame, so we use with_context
    return pl.concat([top_left, bottom_left]).with_context(pl.concat([top_right, bottom_right])).select(
        pl.all()
    )



def overlap(
    df: pl.LazyFrame,
    df2: pl.LazyFrame,
    starts: str,
    ends: str,
    starts_2: str,
    ends_2: str,
) -> pl.LazyFrame:
    sorted_collapsed = df.sort(starts, ends).select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(starts_2, ends_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross")

    df_2_column_names_after_join = j.columns[len(df.columns) :]
    starts_2_renamed = df_2_column_names_after_join[df2.columns.index(starts_2)]
    ends_2_renamed = df_2_column_names_after_join[df2.columns.index(ends_2)]

    return (
        j.with_columns(
            find_starts_in_ends(starts, ends, starts_2_renamed, ends_2_renamed)
        )
        .with_columns(compute_masks())
        .select(pl.all())
        .select(
            pl.col(df.columns).explode().filter(pl.col(MASK_2IN1_PROPERTY).explode())
        )
    )


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
        distance_col: Optional[str] = None
):
    _distance_col = "distance" if distance_col is None else distance_col

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
        distance_col: Optional[str] = None
):
    sorted_collapsed = df.select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(starts_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross", suffix=suffix)

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


ROW_NUMBER = "__row_number__"

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
        lazy_df = lazy_df.select(pl.all().implode()).with_row_count(ROW_NUMBER).select(pl.all().explode())
        grpby_ks = [ROW_NUMBER]
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
        cluster_frame = cluster_frame.drop(ROW_NUMBER)

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
