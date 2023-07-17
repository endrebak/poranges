import polars as pl
import poranges.register_interval_namespace
from poranges import ops

from poranges.closest_intervals import ClosestIntervals
from poranges.groupby_join_result import GroupByJoinResult

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

j = GroupByJoinResult(
    df.lazy(),
    df2.lazy(),
    starts="starts",
    ends="ends",
    starts_2="starts",
    ends_2="ends",
    suffix="_right",
    by=None
)


def test_closest_nonoverlapping_left():
    res = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="D",
        direction="left",
        include_overlapping=False
    )
    expected_result = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [4, 7, 7, 10, 10], dtype=pl.Int64),
            pl.Series("ends", [5, 8, 8, 11, 11], dtype=pl.Int64),
            pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts_right", [0, 0, 5, 6, 8], dtype=pl.Int64),
            pl.Series("ends_right", [2, 2, 7, 9, 10], dtype=pl.Int64),
            pl.Series("D", [3, 6, 1, 2, 1], dtype=pl.UInt64),
        ]
    )

    assert res.collect().frame_equal(expected_result)


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
    print(res.collect())
    assert res.collect()["int_range"].to_list() == [1, 2, 1, 2, 3]


def test_closest_nonoverlapping_right():
    res = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance",
        direction="right",
        include_overlapping=False
    )
    expected_result = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [1, 1, 4, 4, 7], dtype=pl.Int64),
            pl.Series("ends", [4, 4, 5, 5, 8], dtype=pl.Int64),
            pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts_right", [5, 6, 5, 6, 8], dtype=pl.Int64),
            pl.Series("ends_right", [7, 10, 7, 10, 9], dtype=pl.Int64),
            pl.Series("distance", [2, 3, 1, 2, 1], dtype=pl.UInt64),
        ]
    )
    assert res.collect().frame_equal(expected_result)


def test_closest():
    res = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance"
    ).collect().sort(
        by=["chromosome", "starts", "ends", "chromosome_right", "starts_right", "ends_right"]
    )
    expected = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [1, 1, 4, 4, 7, 7, 10, 10], dtype=pl.Int64),
            pl.Series("ends", [4, 4, 5, 5, 8, 8, 11, 11], dtype=pl.Int64),
            pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts_right", [0, 5, 5, 6, 5, 6, 6, 8], dtype=pl.Int64),
            pl.Series("ends_right", [2, 7, 7, 10, 7, 10, 9, 10], dtype=pl.Int64),
            pl.Series("distance", [0, 2, 1, 2, 1, 0, 2, 1], dtype=pl.UInt64),
        ]
    )
    assert res.sort(res.columns).frame_equal(expected)


def test_closest_groupby():
    df = pl.DataFrame(
        {
            "chromosome": ["chr2", "chr1", "chr1", "chr2"],
            "starts": [4, 1, 10, 7],
            "ends": [5, 4, 11, 8],
        }
    )

    df2 = pl.DataFrame(
        {
            "chromosome": ["chr1", "chr2", "chr1", "chr1"],
            "starts": [5, 0, 8, 6],
            "ends": [7, 2, 9, 10],
        }
    )
    right = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance",
        by=["chromosome"],
        direction="right"
    ).collect()

    expected_right = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [1, 1], dtype=pl.Int64),
            pl.Series("ends", [4, 4], dtype=pl.Int64),
            pl.Series("starts_right", [5, 6], dtype=pl.Int64),
            pl.Series("ends_right", [7, 10], dtype=pl.Int64),
            pl.Series("distance", [2, 3], dtype=pl.UInt64),
        ]
    )
    assert right.frame_equal(expected_right)

    left = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance",
        by=["chromosome"],
        direction="left"
    ).collect().sort("chromosome", "starts", "ends", "starts_right", "ends_right")
    expected_left = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr2', 'chr2'], dtype=pl.Utf8),
            pl.Series("starts", [10, 10, 4, 7], dtype=pl.Int64),
            pl.Series("ends", [11, 11, 5, 8], dtype=pl.Int64),
            pl.Series("starts_right", [6, 8, 0, 0], dtype=pl.Int64),
            pl.Series("ends_right", [9, 10, 2, 2], dtype=pl.Int64),
            pl.Series("distance", [2, 1, 3, 6], dtype=pl.UInt64),
        ]
    )

    assert left.frame_equal(expected_left)
