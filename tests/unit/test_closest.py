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
    res = ClosestIntervals(
        j=j,
        k=2,
        distance_col="D"
    ).closest_nonoverlapping_left()
    print(res.collect().to_init_repr())
    expected_result = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [4, 7, 7, 10, 10], dtype=pl.Int64),
            pl.Series("ends", [5, 8, 8, 11, 11], dtype=pl.Int64),
            pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts_right", [0, 0, 5, 5, 6], dtype=pl.Int64),
            pl.Series("ends_right", [2, 2, 7, 7, 10], dtype=pl.Int64),
            pl.Series("D", [3, 6, 1, 4, 1], dtype=pl.UInt64),
        ]
    )
    print(expected_result)

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
    res = ClosestIntervals(
        j=j,
        k=2,
        distance_col="distance"
    ).closest_nonoverlapping_right()
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
    print(expected_result)
    assert res.collect().frame_equal(expected_result)


def test_closest():
    res = ClosestIntervals(
        j=j,
        distance_col="distance"
    ).closest(k=2).sort(by=["starts", "ends", "starts_right", "ends_right"])
    print(res.collect().to_init_repr())
    expected = pl.DataFrame(
        [
            pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts", [1, 1, 4, 4, 7, 7, 10, 10], dtype=pl.Int64),
            pl.Series("ends", [4, 4, 5, 5, 8, 8, 11, 11], dtype=pl.Int64),
            pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
            pl.Series("starts_right", [0, 5, 5, 6, 5, 6, 5, 6], dtype=pl.Int64),
            pl.Series("ends_right", [2, 7, 7, 10, 7, 10, 7, 10], dtype=pl.Int64),
            pl.Series("distance", [0, 2, 1, 2, 1, 0, 4, 1], dtype=pl.UInt64),
        ]
    )
    print(expected)
    assert res.collect().sort(res.columns).frame_equal(expected)


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


def otest_closest_groupby():
    res2 = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance",
        by=["chromosome"]
    )
    print(df)
    print(df2)
    print(res2.sort(df.columns).collect())

    expected = pl.DataFrame(
        {"a": []}
    )
    print(expected)
    raise
    assert res2.collect().sort(res2.columns).frame_equal(expected)


def test_closest_seed_319072507021907277107575597502301494047():
    df1 = pl.DataFrame(
        [
            pl.Series("Chromosome", ['chrM', 'chr2', 'chrM', 'chrM', 'chr3', 'chr3', 'chrM', 'chr3'], dtype=pl.Utf8),
            pl.Series("Start", [116455, 90281, 81395, 65896, 458984, 917853, 196993, 131084], dtype=pl.UInt64),
            pl.Series("End", [195213, 116403, 166002, 66409, 459498, 939287, 263026, 197075], dtype=pl.UInt64),
        ]
    )
    df2 = pl.DataFrame(
        [
            pl.Series("Chromosome", ['chr2'], dtype=pl.Utf8),
            pl.Series("Start", [914176], dtype=pl.UInt64),
            pl.Series("End", [957692], dtype=pl.UInt64),
        ]
    )

    res = df1.interval.closest(
        df2,
        on=("Start", "End"),
        k=2,
        distance_col="distance",
        by=["Chromosome"]
    )

    print(res.collect())

    pass

# def test_closest_nonoverlapping_right_groupby():
#     res2 = closest_nonoverlapping_right(
#         df.lazy(),
#         df2.lazy(),
#         ends="ends",
#         starts_2="starts",
#         k=2,
#         distance_col="distance",
#         by=["chromosome"]
#     )
#     print(res2.collect().to_init_repr())
#     assert res2.collect().frame_equal(
#         pl.DataFrame(
#             [
#                 pl.Series("chromosome", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
#                 pl.Series("starts", [4, 4, 1, 1, 7], dtype=pl.Int64),
#                 pl.Series("ends", [5, 5, 4, 4, 8], dtype=pl.Int64),
#                 pl.Series("chromosome_right", ['chr1', 'chr1', 'chr1', 'chr1', 'chr1'], dtype=pl.Utf8),
#                 pl.Series("starts_right", [5, 6, 5, 6, 8], dtype=pl.Int64),
#                 pl.Series("ends_right", [7, 10, 7, 10, 9], dtype=pl.Int64),
#                 pl.Series("distance", [1, 2, 2, 3, 1], dtype=pl.Int64),
#             ]
#         )
#     )
