import numpy as np
import polars as pl

df = pl.DataFrame(
    {
        "starts": [4, 1, 10, 7],
        "ends": [5, 6, 11, 8],
    }
)


def merge(
        df: pl.LazyFrame,
        starts: str,
        ends: str,
        merge_bookended: bool = False,
        min_distance: int = 0
) -> pl.DataFrame:
    """
    Merge overlapping intervals in a DataFrame.

    Parameters
    ----------
    df
        DataFrame with columns `starts` and `ends`.
    starts
        Column name of start positions.
    ends
        Column name of end positions.
    merge_bookended
        If True, merge intervals that are bookended by overlapping intervals.
    min_distance
        Minimum distance between intervals to be merged.

    Returns
    -------
    DataFrame
        Merged intervals.
    """

    # ordered = df.sort(starts, ends).select(
    #     pl.all().implode()
    # )
    if not merge_bookended:
        cluster_borders_expr = pl.col("starts").explode().shift(-1).gt(pl.col("ends").explode().add(min_distance)).shift_and_fill(True, periods=1).extend_constant(True, 1)
    else:
        cluster_borders_expr = pl.col("starts").shift(-1).ge(pl.col("ends").add(min_distance)).extend_constant(True, 1).implode()

    ordered = df.sort([starts, ends]).select(
        pl.exclude("ends").implode(),
        pl.col("ends").cummax().implode()
    ).with_columns(
        cluster_borders_expr.alias("cluster_borders").implode()
    ).select(
        pl.col("cluster_borders").explode()
    )
    print(ordered.collect())



def test_merge_intervals():
    res = biomerge_intervals(
        starts=df["starts"].to_numpy(),
        ends=df["ends"].to_numpy(),
    )
    print(res)
    print(merge(df.lazy(), "starts", "ends").collect())
    assert 0


def biomerge_intervals(starts, ends, min_dist=0):
    """
    Merge overlapping intervals.

    Parameters
    ----------
    starts, ends : numpy.ndarray
        Interval coordinates. Warning: if provided as pandas.Series, indices
        will be ignored.

    min_dist : float or None
        If provided, merge intervals separated by this distance or less.
        If None, do not merge non-overlapping intervals. Using
        min_dist=0 and min_dist=None will bring different results.
        bioframe uses semi-open intervals, so interval pairs [0,1) and [1,2)
        do not overlap, but are separated by a distance of 0. Such intervals
        are not merged when min_dist=None, but are merged when min_dist=0.

    Returns
    -------
    cluster_ids : numpy.ndarray
        The indices of interval clusters that each interval belongs to.
    cluster_starts : numpy.ndarray
    cluster_ends : numpy.ndarray
        The spans of the merged intervals.

    Notes
    -----
    From
    https://stackoverflow.com/questions/43600878/merging-overlapping-intervals/58976449#58976449
    """

    starts = np.asarray(starts)
    ends = np.asarray(ends)

    order = np.lexsort([ends, starts])
    starts, ends = starts[order], ends[order]

    ends = np.maximum.accumulate(ends)
    print("acc_ends", ends)
    cluster_borders = np.zeros(len(starts) + 1, dtype=bool)
    cluster_borders[0] = True
    cluster_borders[-1] = True

    if min_dist is not None:
        print(cluster_borders)
        cluster_borders[1:-1] = starts[1:] > ends[:-1] + min_dist
        print(starts[1:])
        print(ends[:-1] + min_dist)
        print(cluster_borders)
    else:
        cluster_borders[1:-1] = starts[1:] >= ends[:-1]

    cluster_ids_sorted = np.cumsum(cluster_borders)[:-1] - 1
    cluster_ids = np.full(starts.shape[0], -1)
    cluster_ids[order] = cluster_ids_sorted

    cluster_starts = starts[:][cluster_borders[:-1]]
    cluster_ends = ends[:][cluster_borders[1:]]

    return cluster_ids, cluster_starts, cluster_ends