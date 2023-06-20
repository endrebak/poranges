from typing import List
import polars as pl
import numpy as np

import bioframe.core.arrops as arrops

import pyoframe as pf

import polars as pl

import pytest


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

DF_COLUMNS_PROPERTY = [STARTS_PROPERTY, ENDS_PROPERTY]
TEMP_CHROMOSOME_PROPERTY = f"__{CHROMOSOME_PROPERTY}__"
DF2_COLUMNS_PROPERTY = [STARTS2_PROPERTY, ENDS2_PROPERTY]


def search(
        col1: str,
        col2: str,
        side: str = "left"
) -> pl.Expr:
    return pl.col(col1).explode().search_sorted(pl.col(col2).explode(), side=side)


def lengths(
        starts: str,
        ends: str,
        outname: str = ""
) -> pl.Expr:
    return pl.col(ends).explode().sub(pl.col(starts).explode()).explode().alias(outname)


def find_starts_in_ends(starts, ends, starts_2, ends_2, closed: bool = False) -> List[pl.Expr]:
    return [
        search(starts_2, starts).alias(STARTS_2IN1_PROPERTY).implode(),
        search(starts_2, ends).alias(ENDS_2IN1_PROPERTY).implode(),
        search(starts, starts_2, side="right").alias(STARTS_1IN2_PROPERTY).implode(),
        search(starts, ends_2).alias(ENDS_1IN2_PROPERTY).implode(),
    ]


def compute_masks() -> List[pl.Expr]:
    return [
        pl.all(),
        pl.col(ENDS_2IN1_PROPERTY).explode().gt(pl.col(STARTS_2IN1_PROPERTY).explode()).implode().alias(MASK_2IN1_PROPERTY),
        pl.col(ENDS_1IN2_PROPERTY).explode().gt(pl.col(STARTS_1IN2_PROPERTY).explode()).implode().alias(MASK_1IN2_PROPERTY),
    ]


def apply_masks() -> List[pl.Expr]:
    return [
        pl.exclude(STARTS_1IN2_PROPERTY, STARTS_2IN1_PROPERTY, ENDS_1IN2_PROPERTY, ENDS_2IN1_PROPERTY),
        pl.col([STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY]).explode().filter(pl.col(MASK_2IN1_PROPERTY).explode()).implode(),
        pl.col([STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY]).explode().filter(pl.col(MASK_1IN2_PROPERTY).explode()).implode()
    ]


def add_lengths() -> pl.Expr:
    return [
        pl.all(),
        pl.col(ENDS_2IN1_PROPERTY).explode().sub(pl.col(STARTS_2IN1_PROPERTY).explode()).alias(LENGTHS_2IN1_PROPERTY).implode(),
        pl.col(ENDS_1IN2_PROPERTY).explode().sub(pl.col(STARTS_1IN2_PROPERTY).explode()).alias(LENGTHS_1IN2_PROPERTY).implode()
    ]


def repeat_frame(columns, mask, startsin, endsin) -> pl.Expr:
    return pl.col(columns).explode().filter(
        pl.col(mask).explode()).repeat_by(
        pl.col(endsin).explode() - pl.col(startsin).explode()
    ).explode()


def repeat_other(columns, starts, diffs):
    return pl.col(columns).explode().take(
        pl.col(starts).explode().repeat_by(pl.col(diffs).explode()).alias("cat_starts").explode().add(
            pl.arange(0, pl.col(diffs).explode().sum()).explode().alias("length_sum_arange").sub(
                pl.col(diffs).explode().cumsum().sub(pl.col(diffs).explode()).repeat_by(pl.col(diffs).explode()).explode()
            )
        )
    )

def join(
       df: pl.LazyFrame,
       df2: pl.LazyFrame,
       suffix: str,
       starts: str,
       ends: str,
       starts_2: str,
       ends_2: str,
):
    sorted_collapsed = df.sort(starts, ends).select([pl.all().implode()])
    sorted_collapsed_2 = df2.sort(starts_2, ends_2).select([pl.all().implode()])
    j = sorted_collapsed.join(sorted_collapsed_2, how="cross", suffix=suffix)
    df_2_column_names_after_join = j.columns[len(df.columns):]

    res = j.with_columns(
        find_starts_in_ends(STARTS_PROPERTY, ENDS_PROPERTY, STARTS2_PROPERTY, ENDS2_PROPERTY)
    ).with_columns(
        compute_masks()
    ).with_columns(
        apply_masks()
    ).with_columns(
        add_lengths()
    ).select(
         pl.concat(
             [
                 repeat_frame(df.columns, MASK_2IN1_PROPERTY, STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY),
                 repeat_other(df.columns, STARTS_1IN2_PROPERTY, LENGTHS_1IN2_PROPERTY)
             ]
         ),
         pl.concat(
             [
                 repeat_other(df_2_column_names_after_join, STARTS_2IN1_PROPERTY, LENGTHS_2IN1_PROPERTY),
                 repeat_frame(df_2_column_names_after_join, MASK_1IN2_PROPERTY, STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY),
             ]
         )
    )
    print(res.collect())
