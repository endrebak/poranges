import polars as pl

from poranges.ops import merge

df = pl.DataFrame(
    {
        "starts": [4, 10, 7, 1],
        "ends": [5, 11, 8, 6],
        "weights": [-42, 0, 2, 1],
    }
)


def test_merge_intervals():
    res = merge(df.lazy(), "starts", "ends").collect()

    assert res.frame_equal(
        pl.DataFrame(
            [
                pl.Series("starts", [1, 7, 10], dtype=pl.Int64),
                pl.Series("ends", [6, 8, 11], dtype=pl.Int64),
                pl.Series("starts_before_merge", [[1, 4], [7], [10]], dtype=pl.List(pl.Int64)),
                pl.Series("ends_before_merge", [[6, 5], [8], [11]], dtype=pl.List(pl.Int64)),
                pl.Series("weights", [[1, -42], [2], [0]], dtype=pl.List(pl.Int64)),
            ]
        )
    )


df = pl.DataFrame(
    {
        "chrom": [1, 1, 1, 1, 2],
        "starts": [4, 10, 7, 1, 0],
        "ends": [5, 11, 8, 6, 100],
        "weights": [-42, 0, 2, 1, 3],
    }
)


def test_merge_intervals_groupby():
    # sorting on chrom since groupby keys might be out of order
    res = merge(df.lazy(), "starts", "ends", by=["chrom"]).collect().sort("chrom")

    expected = pl.DataFrame(
        [
            pl.Series("chrom", [1, 1, 1, 2]),
            pl.Series("starts", [1, 7, 10, 0], dtype=pl.Int64),
            pl.Series("ends", [6, 8, 11, 100], dtype=pl.Int64),
            pl.Series("starts_before_merge", [[1, 4], [7], [10], [0]], dtype=pl.List(pl.Int64)),
            pl.Series("ends_before_merge", [[6, 5], [8], [11], [100]], dtype=pl.List(pl.Int64)),
            pl.Series("weights", [[1, -42], [2], [0], [3]], dtype=pl.List(pl.Int64)),
        ]
    )

    assert res.frame_equal(expected)


def test_merge_intervals_empty():
    expected = pl.LazyFrame({}, schema=[("col1", pl.Float32), ("col2", pl.Int64)])
    res = merge(expected, "col1", "col2").collect()

    assert res.frame_equal(
        pl.DataFrame(
            schema=[("col1", pl.Float32), ("col2", pl.Int64), ("col1_before_merge", pl.Float32), ("col2_before_merge", pl.Int64)]
        )
    )
