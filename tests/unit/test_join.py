from datetime import date
import poranges.register_interval_namespace

import polars as pl

from poranges.groupby_join_result import GroupByJoinResult

CHROMOSOME_PROPERTY = "chromosome"
CHROMOSOME2_PROPERTY = "chromosome_2"
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

df = pl.DataFrame(
    {
        CHROMOSOME_PROPERTY: ["chr1", "chr1", "chr1", "chr1"],
        STARTS_PROPERTY: [0, 8, 6, 5],
        ENDS_PROPERTY: [6, 9, 10, 7],
    }
)

DF1_COLUMNS = df.columns

df2 = pl.DataFrame(
    {
        CHROMOSOME_PROPERTY: ["chr1", "chr1", "chr1"],
        STARTS_PROPERTY: [6, 3, 1],
        ENDS_PROPERTY: [7, 8, 2],
        "genes": ["a", "b", "c"],
    }
)

DF2_COLUMNS = df2.columns


def chromosome_join(df, df2):
    return (
        df.lazy()
        .sort("starts", ENDS_PROPERTY)
        .select([pl.all().implode()])
        .join(
            df2.lazy().sort("starts", ENDS_PROPERTY).select([pl.all().implode()]),
            how="cross",
            suffix="_2",
        )
    )


def test_join():
    res = df.interval.join(
        df2.lazy(), on=("starts", "ends"), suffix="_2"
    )
    expected = pl.DataFrame(
        [
            pl.Series(
                "chromosome",
                ["chr1", "chr1", "chr1", "chr1", "chr1", "chr1"],
                dtype=pl.Utf8,
            ),
            pl.Series("starts", [0, 0, 5, 6, 5, 6], dtype=pl.Int64),
            pl.Series("ends", [6, 6, 7, 10, 7, 10], dtype=pl.Int64),
            pl.Series(
                "chromosome_2",
                ["chr1", "chr1", "chr1", "chr1", "chr1", "chr1"],
                dtype=pl.Utf8,
            ),
            pl.Series("starts_2", [1, 3, 6, 6, 3, 3], dtype=pl.Int64),
            pl.Series("ends_2", [2, 8, 7, 7, 8, 8], dtype=pl.Int64),
            pl.Series("genes", ["c", "b", "a", "a", "b", "b"], dtype=pl.Utf8),
        ]
    )
    print(res.collect())
    print(expected)

    assert res.collect().frame_equal(expected)


def test_time():
    df_1 = pl.DataFrame(
        {
            "id": ["1", "3", "2"],
            "start": [
                date(2022, 1, 1),
                date(2022, 5, 11),
                date(2022, 3, 4),
            ],
            "end": [
                date(2022, 2, 4),
                date(2022, 5, 16),
                date(2022, 3, 10),
            ],
        }
    )
    df_2 = pl.DataFrame(
        {
            "start": [
                date(2021, 12, 31),
                date(2025, 12, 31),
            ],
            "end": [
                date(2022, 4, 1),
                date(2025, 4, 1),
            ],
        }
    )

    expected = pl.DataFrame(
        [
            pl.Series("id", ["1", "2"], dtype=pl.Utf8),
            pl.Series("start", [date(2022, 1, 1), date(2022, 3, 4)], dtype=pl.Date),
            pl.Series("end", [date(2022, 2, 4), date(2022, 3, 10)], dtype=pl.Date),
            pl.Series(
                "start_whatevz", [date(2021, 12, 31), date(2021, 12, 31)], dtype=pl.Date
            ),
            pl.Series(
                "end_whatevz", [date(2022, 4, 1), date(2022, 4, 1)], dtype=pl.Date
            ),
        ]
    )

    res = df_1.interval.join(df_2, on=("start", "end"), suffix="_whatevz")
    assert res.collect().frame_equal(expected)


def test_overlap():
    res = df.interval.overlap(df2.lazy(), on=("starts", "ends"))
    a = res.collect().sort("starts")
    expected = pl.DataFrame(
        {"chromosome": ["chr1"] * 3, "starts": [0, 5, 6], "ends": [6, 7, 10]}
    )

    assert a.frame_equal(expected)


def test_join_groupby():
    df = pl.DataFrame(
        {
            "k": ["A", "B", "A"],
            "a": [1, 0, 30, ],
            "b": [2, 7, 40, ]
        }
    )
    df2 = pl.LazyFrame(
        {
            "k": ["B", "A", "C", "B"],
            "a": [6, 0, 5, 29],
            "b": [10, 3, 6, 30]
        }
    )

    res = df.interval.join(df2.lazy(), on=("a", "b"), by="k", suffix="_2").collect().sort("k", descending=True)
    print(res)
    expected_result = pl.DataFrame(
        [
            pl.Series("k", ['B', 'A'], dtype=pl.Utf8),
            pl.Series("a", [0, 1], dtype=pl.Int64),
            pl.Series("b", [7, 2], dtype=pl.Int64),
            pl.Series("a_2", [6, 0], dtype=pl.Int64),
            pl.Series("b_2", [10, 3], dtype=pl.Int64),
        ]
    )
    print(expected_result)

    assert res.frame_equal(expected_result)


def test_join_groupby_2():
    df = pl.LazyFrame(
        {
            "k": ["A", "B", "A", "A"],
            "a": [1, 0, 28, 100],
            "b": [2, 7, 40, 200]
        }
    )
    df2 = pl.LazyFrame(
        {
            "k": ["B", "A", "C", "A", "A"],
            "a": [6, 0, 5, 29, 0],
            "b": [10, 3, 6, 30, 300]
        }
    )

    res = df.interval.join(df2, on=("a", "b"), by="k").collect().sort("k", "a", "b", "a_right", "b_right", descending=False)

    print(res)
    print(res.to_init_repr())
    expected_result = pl.DataFrame(
        [
            pl.Series("k", ['A', 'A', 'A', 'A', 'A', 'B'], dtype=pl.Utf8),
            pl.Series("a", [1, 1, 28, 28, 100, 0], dtype=pl.Int64),
            pl.Series("b", [2, 2, 40, 40, 200, 7], dtype=pl.Int64),
            pl.Series("a_right", [0, 0, 0, 29, 0, 6], dtype=pl.Int64),
            pl.Series("b_right", [3, 300, 300, 30, 300, 10], dtype=pl.Int64),
        ]
    )
    print(expected_result)

    assert res.frame_equal(expected_result)