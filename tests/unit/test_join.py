from datetime import date
import poranges.register_interval_namespace

import polars as pl

from poranges.ops import (
    compute_masks,
    repeat_other,
    mask_and_repeat_frame,
    apply_masks,
    add_lengths,
    find_starts_in_ends,
    join,
)

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


g = chromosome_join(df, df2)

SCHEMA = {
    "chromosome": pl.List(pl.Utf8),
    "starts": pl.List(pl.Int64),
    "ends": pl.List(pl.Int64),
    "starts_2": pl.List(pl.Int64),
    "ends_2": pl.List(pl.Int64),
    "genes": pl.List(pl.Utf8),
    "starts_2in1": pl.List(pl.UInt32),
    "ends_2in1": pl.List(pl.UInt32),
    "starts_1in2": pl.List(pl.UInt32),
    "ends_1in2": pl.List(pl.UInt32),
}

expected_result_starts_in_ends = pl.DataFrame(
    {
        CHROMOSOME_PROPERTY: [["chr1"] * 4],
        STARTS_PROPERTY: [[0, 5, 6, 8]],
        ENDS_PROPERTY: [[6, 7, 10, 9]],
        STARTS2_PROPERTY: [[1, 3, 6]],
        ENDS2_PROPERTY: [[2, 8, 7]],
        "genes": [["c", "b", "a"]],
        STARTS_2IN1_PROPERTY: [[0, 2, 2, 3]],
        ENDS_2IN1_PROPERTY: [[2, 3, 3, 3]],
        STARTS_1IN2_PROPERTY: [[1, 1, 3]],
        ENDS_1IN2_PROPERTY: [[1, 3, 3]],
    },
    schema=SCHEMA,
)

SCHEMA_2 = SCHEMA.copy()
SCHEMA_2[MASK_2IN1_PROPERTY] = pl.List(pl.Boolean)
SCHEMA_2[MASK_1IN2_PROPERTY] = pl.List(pl.Boolean)

expected_result_compute_masks = pl.DataFrame(
    {
        CHROMOSOME_PROPERTY: [["chr1"] * 4],
        STARTS_PROPERTY: [[0, 5, 6, 8]],
        ENDS_PROPERTY: [[6, 7, 10, 9]],
        STARTS2_PROPERTY: [[1, 3, 6]],
        ENDS2_PROPERTY: [[2, 8, 7]],
        "genes": [["c", "b", "a"]],
        STARTS_2IN1_PROPERTY: [[0, 2, 2, 3]],
        ENDS_2IN1_PROPERTY: [[2, 3, 3, 3]],
        STARTS_1IN2_PROPERTY: [[1, 1, 3]],
        ENDS_1IN2_PROPERTY: [[1, 3, 3]],
        MASK_1IN2_PROPERTY: [[False, True, False]],
        MASK_2IN1_PROPERTY: [[True, True, True, False]],
    },
    schema=SCHEMA_2,
)

expected_result_apply_masks = pl.DataFrame(
    {
        CHROMOSOME_PROPERTY: [["chr1"] * 4],
        STARTS_PROPERTY: [[0, 5, 6, 8]],
        ENDS_PROPERTY: [[6, 7, 10, 9]],
        STARTS2_PROPERTY: [[1, 3, 6]],
        ENDS2_PROPERTY: [[2, 8, 7]],
        "genes": [["c", "b", "a"]],
        STARTS_2IN1_PROPERTY: [[0, 2, 2]],
        ENDS_2IN1_PROPERTY: [[2, 3, 3]],
        STARTS_1IN2_PROPERTY: [[1]],
        ENDS_1IN2_PROPERTY: [[3]],
        MASK_1IN2_PROPERTY: [[False, True, False]],
        MASK_2IN1_PROPERTY: [[True, True, True, False]],
    },
    schema=SCHEMA_2,
)


def test_find_starts_in_ends():
    result = g.with_columns(
        find_starts_in_ends(
            STARTS_PROPERTY, ENDS_PROPERTY, STARTS2_PROPERTY, ENDS2_PROPERTY
        )
    ).collect()
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
    assert result.frame_equal(expected_result_starts_in_ends)


def test_compute_masks():
    result = expected_result_starts_in_ends.with_columns(compute_masks())
    assert result.frame_equal(expected_result_compute_masks)


def test_apply_masks():
    result = expected_result_compute_masks.with_columns(apply_masks())
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
    assert result.frame_equal(expected_result_apply_masks)


def test_add_lengths():
    result = expected_result_apply_masks.with_columns(add_lengths())
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
    assert result[LENGTHS_2IN1_PROPERTY].explode().to_list() == [2, 1, 1]


def test_repeat_other():
    res = (
        expected_result_apply_masks.with_columns([pl.lit([[2, 1, 1]]).alias("diffs")])
        .lazy()
        .select(
            repeat_other(
                ["starts_2", "ends_2"],
                pl.col(STARTS_2IN1_PROPERTY).explode(),
                pl.col("diffs").explode()
            )
        )
    )
    res.collect().frame_equal(
        pl.DataFrame({STARTS2_PROPERTY: [1, 3, 6, 6], ENDS2_PROPERTY: [2, 8, 7, 7]})
    )


def test_repeat_frame():
    res = expected_result_apply_masks.select(
        mask_and_repeat_frame(
            [STARTS_PROPERTY, ENDS_PROPERTY],
            MASK_2IN1_PROPERTY,
            STARTS_2IN1_PROPERTY,
            ENDS_2IN1_PROPERTY,
        )
    )
    with pl.Config() as cfg:
        cfg.set_tbl_cols(-1)
    assert res.frame_equal(
        pl.DataFrame({STARTS_PROPERTY: [0, 0, 5, 6], ENDS_PROPERTY: [6, 6, 7, 10]})
    )


def test_join():
    res = join(
        df.lazy(),
        df2.lazy(),
        "_2",
        STARTS_PROPERTY,
        ENDS_PROPERTY,
        STARTS_PROPERTY,
        ENDS_PROPERTY,
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
            pl.Series("starts_2", [1, 3, 6, 6, 3, 3], dtype=pl.Int64),
            pl.Series("ends_2", [2, 8, 7, 7, 8, 8], dtype=pl.Int64),
            pl.Series("genes", ["c", "b", "a", "a", "b", "b"], dtype=pl.Utf8),
        ]
    )

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
    a = res.collect()
    expected = pl.DataFrame(
        {"chromosome": ["chr1"] * 3, "starts": [0, 5, 6], "ends": [6, 7, 10]}
    )

    assert a.frame_equal(expected)
