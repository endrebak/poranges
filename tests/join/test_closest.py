import polars as pl
import pandas as pd
import numpy as np

from pyoframe import ops
from pyoframe.ops import repeat_other, add_lengths, add_length

df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [4, 1, 10, 7],
        "ends": [5, 4, 11, 8],
    }
)

df2 = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [5, 0, 8, 6],
        "ends": [7, 2, 9, 10],
    }
)

# [[0 1]
#  [2 3]
#  [3 0]]


def closest_nonoverlapping_left(
    df: pl.LazyFrame,
    df2: pl.LazyFrame,
    *,
    starts: str,
    ends_2: str,
    suffix: str = "_right",
    k: int = 2
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
                ops.search(ends_2_renamed, starts, side="right")
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
                ops.arange_multi(
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
    return res


def closest_nonoverlapping_right(
        df: pl.LazyFrame,
        df2: pl.LazyFrame,
        *,
        ends: str,
        starts_2: str,
        suffix: str = "_right",
        k: int = 2
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
                ops.search(starts_2_renamed, ends, side="left")
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
                ops.arange_multi(
                    starts=pl.col(right_closest_startidx).explode(),
                    diffs=pl.col(diffs).explode(),
                ).alias("arange").implode()
            ]
        )
    ).select(
        pl.col(df.columns).explode().repeat_by(pl.col(diffs).explode()).explode().drop_nulls(),
        pl.col(df_2_column_names_after_join).explode().take(pl.col("arange").explode().cast(pl.UInt32))
     )

    return res


def test_closest_nonoverlapping_left():
    res2 = closest_nonoverlapping_left(
        df.lazy(),
        df2.lazy(),
        starts="starts",
        ends_2="ends"
    )

    assert res2.collect().frame_equal(
        pl.DataFrame(
            [
                pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
                pl.Series("starts", [4, 10, 10, 7, 7], dtype=pl.Int64),
                pl.Series("ends", [5, 11, 11, 8, 8], dtype=pl.Int64),
                pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
                pl.Series("starts_right", [0, 8, 6, 0, 5], dtype=pl.Int64),
                pl.Series("ends_right", [2, 9, 10, 2, 7], dtype=pl.Int64),
            ]
        )
    )


def test_arange_multi():
    df = pl.DataFrame(
        {
            "starts": pl.Series([1, 1, 4, 3]),
            "lengths": pl.Series([2, 2, 0, 1])
        }
    ).lazy()
    res = df.select(
        ops.arange_multi(
            starts=pl.col("starts").filter(pl.col("lengths").gt(0)),
            diffs=pl.col("lengths").filter(pl.col("lengths").gt(0))
        )
    )
    assert res.collect()["starts"].to_list() == [1, 2, 1, 2, 3]


def test_closest_nonoverlapping_right():
    print(
        _closest_intervals_nooverlap(
            df["starts"],
            df["ends"],
            df2["starts"],
            df2["ends"],
            "right",
            k=2
        )
    )
    res2 = closest_nonoverlapping_right(
        df.lazy(),
        df2.lazy(),
        ends="ends",
        starts_2="starts",
        k=2
    )
    print(res2.collect().to_init_repr())
    assert res2.collect().frame_equal(
        pl.DataFrame(
            [
                pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
                pl.Series("starts", [4, 4, 1, 1, 7], dtype=pl.Int64),
                pl.Series("ends", [5, 5, 4, 4, 8], dtype=pl.Int64),
                pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
                pl.Series("starts_right", [5, 6, 5, 6, 8], dtype=pl.Int64),
                pl.Series("ends_right", [7, 10, 7, 10, 9], dtype=pl.Int64),
            ]
        )
    )
    # assert res2.collect().frame_equal(
    #     pl.DataFrame(
    #         [
    #             pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
    #             pl.Series("starts", [4, 10, 10, 7, 7], dtype=pl.Int64),
    #             pl.Series("ends", [5, 11, 11, 8, 8], dtype=pl.Int64),
    #             pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
    #             pl.Series("starts_right", [0, 8, 6, 0, 5], dtype=pl.Int64),
    #             pl.Series("ends_right", [2, 9, 10, 2, 7], dtype=pl.Int64),
    #         ]
    #     )
    # )


def _closest_intervals_nooverlap(
    starts1, ends1, starts2, ends2, direction, tie_arr=None, k=1
):
    """
    For every interval in set 1, return the indices of k closest intervals
    from set 2 to the left from the interval (with smaller coordinate).
    Overlapping intervals from set 2 are not reported, unless they overlap by
    a single point.

    Parameters
    ----------
    starts1, ends1, starts2, ends2 : numpy.ndarray
        Interval coordinates. Warning: if provided as pandas.Series, indices
        will be ignored.

    direction : str ("left" or "right")
        Orientation of closest interval search

    tie_arr : numpy.ndarray or None
        Extra data describing intervals in set 2 to break ties when multiple
        intervals are located at the same distance. An interval with the
        *lowest* value is selected.

    k : int
        The number of neighbors to report.

    Returns
    -------
    ids: numpy.ndarray
        One Nx2 array containing the indices of pairs of closest intervals,
        reported for the neighbors in specified direction (by genomic
        coordinate). The two columns are the inteval ids from set 1, ids of
        the closest intevals from set 2.

    """

    for vec in [starts1, ends1, starts2, ends2]:
        if isinstance(vec, pd.Series):
            warnings.warn(
                "One of the inputs is provided as pandas.Series "
                "and its index will be ignored.",
                SyntaxWarning,
            )

    starts1 = np.asarray(starts1)
    ends1 = np.asarray(ends1)
    starts2 = np.asarray(starts2)
    ends2 = np.asarray(ends2)

    n1 = starts1.shape[0]
    n2 = starts2.shape[0]

    ids = np.zeros((0, 2), dtype=int)

    if k > 0 and direction == "left":
        if tie_arr is None:
            ends2_sort_order = np.argsort(ends2)
        else:
            ends2_sort_order = np.lexsort([-tie_arr, ends2])

        ids2_endsorted = np.arange(0, n2)[ends2_sort_order]
        ends2_sorted = ends2[ends2_sort_order]

        left_closest_endidx = np.searchsorted(ends2_sorted, starts1, "right")
        left_closest_startidx = np.maximum(left_closest_endidx - k, 0)
        print("left_closest_startidx", left_closest_startidx)
        print("left_closest_endidx", left_closest_endidx)

        int1_ids = np.repeat(np.arange(n1), left_closest_endidx - left_closest_startidx)
        int2_sorted_ids = arange_multi(left_closest_startidx, left_closest_endidx)
        print("int2_sorted_ids", int2_sorted_ids)

        ids = np.vstack(
            [
                int1_ids,
                ids2_endsorted[int2_sorted_ids],
                # ends2_sorted[int2_sorted_ids] - starts1[int1_ids],
                # arange_multi(left_closest_startidx - left_closest_endidx, 0)
            ]
        ).T
        print(ids)

    elif k > 0 and direction == "right":
        if tie_arr is None:
            starts2_sort_order = np.argsort(starts2)
        else:
            starts2_sort_order = np.lexsort([tie_arr, starts2])

        ids2_startsorted = np.arange(0, n2)[starts2_sort_order]
        starts2_sorted = starts2[starts2_sort_order]

        right_closest_startidx = np.searchsorted(starts2_sorted, ends1, "left")
        right_closest_endidx = np.minimum(right_closest_startidx + k, n2)
        print("right_closest_startidx", right_closest_startidx)
        print("right_closest_endidx", right_closest_endidx)

        int1_ids = np.repeat(
            np.arange(n1), right_closest_endidx - right_closest_startidx
        )
        int2_sorted_ids = arange_multi(right_closest_startidx, right_closest_endidx)
        print("int2_sorted_ids", int2_sorted_ids)
        print(f"     n2 {n2}")
        print(f"     k {k}")
        ids = np.vstack(
            [
                int1_ids,
                ids2_startsorted[int2_sorted_ids],
                #  starts2_sorted[int2_sorted_ids] - ends1[int1_ids],
                #  arange_multi(1, right_closest_endidx -
                #                  right_closest_startidx + 1)
            ]
        ).T

    return ids


def arange_multi(starts, stops=None, lengths=None):
    """
    Create concatenated ranges of integers for multiple start/length.

    Parameters
    ----------
    starts : numpy.ndarray
        Starts for each range
    stops : numpy.ndarray
        Stops for each range
    lengths : numpy.ndarray
        Lengths for each range. Either stops or lengths must be provided.

    Returns
    -------
    concat_ranges : numpy.ndarray
        Concatenated ranges.

    Notes
    -----
    See the following illustrative example:

    starts = np.array([1, 3, 4, 6])
    stops = np.array([1, 5, 7, 6])

    print arange_multi(starts, lengths)
    >>> [3 4 4 5 6]

    From: https://codereview.stackexchange.com/questions/83018/vectorized-numpy-version-of-arange-with-multiple-start-stop

    """

    if (stops is None) == (lengths is None):
        raise ValueError("Either stops or lengths must be provided!")

    if lengths is None:
        lengths = stops - starts
    print("lengths", lengths)

    if np.isscalar(starts):
        starts = np.full(len(stops), starts)

    # Repeat start position index length times and concatenate
    cat_start = np.repeat(starts, lengths)

    # Create group counter that resets for each start/length
    cat_counter = np.arange(lengths.sum()) - np.repeat(
        lengths.cumsum() - lengths, lengths
    )

    # Add group counter to group specific starts
    cat_range = cat_start + cat_counter
    print("cat_range", cat_range)

    return cat_range
