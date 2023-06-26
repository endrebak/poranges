import polars as pl

from pyoframe import ops
from pyoframe.ops import closest_nonoverlapping_left, \
    closest_nonoverlapping_right

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


def test_closest_nonoverlapping_left():
    res2 = closest_nonoverlapping_left(
        df.lazy(),
        df2.lazy(),
        starts="starts",
        ends_2="ends",
        k=2
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
    res2 = closest_nonoverlapping_right(
        df.lazy(),
        df2.lazy(),
        ends="ends",
        starts_2="starts",
        k=2,
        distance_col="distance"
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
                pl.Series("distance", [1, 2, 2, 3, 1], dtype=pl.Int64),
            ]
        )
    )


def test_closest():
    res2 = df.interval.closest(
        df2,
        on=("starts", "ends"),
        k=2,
        distance_col="distance"
    )
    print(res2.collect().to_init_repr())
    raise
