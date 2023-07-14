from typing import List, Literal, TYPE_CHECKING, Optional

import polars as pl

from poranges.constants import STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY, STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY, \
    MASK_2IN1_PROPERTY, MASK_1IN2_PROPERTY, LENGTHS_2IN1_PROPERTY, LENGTHS_1IN2_PROPERTY
from poranges.ops import search, add_length

if TYPE_CHECKING:
    from poranges.groupby_join_result import GroupByJoinResult


class OverlappingIntervals:
    """The data needed to compute paired overlaps between two sets of ranges.

    This class is used to compute things like joins and overlaps between ranges."""
    closed_intervals: bool

    def __init__(
            self,
            j: "GroupByJoinResult",
            closed_intervals: bool = False,
    ) -> None:
        grpby_ks = j.by
        self.j = j
        self.closed_intervals = closed_intervals

        self.data = (
            j.joined.groupby(grpby_ks).agg(
                [pl.all().explode()] + self.find_starts_in_ends()
            )
            .groupby(grpby_ks).agg(
                [pl.all().explode()] + self.compute_masks()
            )
            .groupby(grpby_ks).agg(
                [
                    pl.exclude(
                        STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY, STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY,
                    ).explode()
                ] + self.apply_masks()
            )
            .groupby(grpby_ks).agg(
                [pl.all()] + self.add_lengths()
            )
            .explode(pl.exclude(grpby_ks))

        )

    @staticmethod
    def lengths(starts: str, ends: str, outname: str = "") -> pl.Expr:
        return pl.col(ends).explode().sub(pl.col(starts).explode()).explode().alias(outname)

    def find_starts_in_ends(self) -> List[pl.Expr]:
        side: Literal["right", "left"] = "right" if self.closed_intervals else "left"  # type: ignore

        return [
            search(self.j.starts_2_renamed, self.j.starts, side="left").alias(STARTS_2IN1_PROPERTY),
            search(self.j.starts_2_renamed, self.j.ends, side=side).alias(ENDS_2IN1_PROPERTY),
            search(self.j.starts, self.j.starts_2_renamed, side="right").alias(STARTS_1IN2_PROPERTY),
            search(self.j.starts, self.j.ends_2_renamed, side=side).alias(ENDS_1IN2_PROPERTY),
        ]

    @staticmethod
    def compute_masks() -> List[pl.Expr]:
        return [
            pl.col(ENDS_2IN1_PROPERTY)
            .explode()
            .gt(pl.col(STARTS_2IN1_PROPERTY).explode())
            .alias(MASK_2IN1_PROPERTY),
            pl.col(ENDS_1IN2_PROPERTY)
            .explode()
            .gt(pl.col(STARTS_1IN2_PROPERTY).explode())
            .alias(MASK_1IN2_PROPERTY),
        ]

    @staticmethod
    def apply_masks() -> List[pl.Expr]:
        return [
            pl.col([STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY])
            .explode()
            .filter(pl.col(MASK_2IN1_PROPERTY).explode()),
            pl.col([STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY])
            .explode()
            .filter(pl.col(MASK_1IN2_PROPERTY).explode())
        ]


    @staticmethod
    def add_lengths() -> List[pl.Expr]:
        return (
            [
                add_length(STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY, LENGTHS_2IN1_PROPERTY),
                add_length(STARTS_1IN2_PROPERTY, ENDS_1IN2_PROPERTY, LENGTHS_1IN2_PROPERTY)
            ]
        )

    @staticmethod
    def repeat_frame(columns, startsin, endsin) -> pl.Expr:
        return (
            pl.col(columns)
            .explode()
            .repeat_by(pl.col(endsin).explode() - pl.col(startsin).explode())
            .explode()
        )

    @staticmethod
    def mask_and_repeat_frame(columns, mask, startsin, endsin) -> pl.Expr:
        return (
            pl.col(columns).explode()
            .filter(pl.col(mask).explode())
            .repeat_by(
                pl.when(pl.col(mask).list.any()).then(
                    (pl.col(endsin).explode().drop_nulls() - pl.col(startsin).explode().drop_nulls())
                ).otherwise(
                    pl.lit(0)
                )
            ).explode()
        )

    @staticmethod
    def repeat_other(columns, starts, diffs):
        return (
            pl.col(columns)
            .explode()
            .take(
                pl.int_ranges(
                    start=starts,
                    end=starts.add(diffs)
                ).explode().drop_nulls()
            )
        )

    def overlapping_pairs(self) -> "pl.LazyFrame":
        if self.j.empty():
            return self.j.joined

        grpby_ks = self.j.by
        df_2_column_names_after_join = self.j.colnames_df2_after_join()
        df_column_names_without_groupby_ks = self.j.colnames_without_groupby_ks()
        df_2_column_names_without_groupby_ks = self.j.colnames_2_without_groupby_ks()

        top_left = (
            self.data
            .filter(pl.col(MASK_2IN1_PROPERTY).list.any())
            .groupby(grpby_ks).agg(
                self.mask_and_repeat_frame(
                    [c for c in df_column_names_without_groupby_ks if
                     c not in [MASK_2IN1_PROPERTY, STARTS_2IN1_PROPERTY, ENDS_2IN1_PROPERTY]],
                    mask=MASK_2IN1_PROPERTY,
                    startsin=STARTS_2IN1_PROPERTY,
                    endsin=ENDS_2IN1_PROPERTY
                )
            ).explode(df_column_names_without_groupby_ks).drop_nulls()
        ).sort(grpby_ks)
        # print("top_left\n", top_left.collect())

        bottom_left = (
            self.data
            .groupby(grpby_ks).agg(
                self.repeat_other(
                    df_column_names_without_groupby_ks, pl.col(STARTS_1IN2_PROPERTY).explode(),
                    pl.col(LENGTHS_1IN2_PROPERTY).explode()
                )
            ).explode(df_column_names_without_groupby_ks).drop_nulls()
        ).sort(grpby_ks)
        # print("bottom_left\n", bottom_left.collect())

        top_right = (
            self.data
            .groupby(grpby_ks).agg(
                self.repeat_other(
                    df_2_column_names_after_join,
                    pl.col(STARTS_2IN1_PROPERTY).explode(),
                    pl.col(LENGTHS_2IN1_PROPERTY).explode(),
                )
            ).explode(df_2_column_names_without_groupby_ks).drop_nulls()
        ).sort(grpby_ks)

        # print(
        #     self.data.groupby(grpby_ks).agg(
        #         pl.col(
        #             df_2_column_names_after_join + [
        #                 STARTS_2IN1_PROPERTY,
        #                 LENGTHS_2IN1_PROPERTY,
        #             ]
        #         ).explode()
        #     ).collect()
        # )
        # print(df_2_column_names_after_join)
        # print("top_right\n", top_right.collect())
        bottom_right = (
            self.data
            .filter(pl.col(MASK_1IN2_PROPERTY).list.any())
            .groupby(grpby_ks).agg(
                self.mask_and_repeat_frame(
                    df_2_column_names_after_join,
                    MASK_1IN2_PROPERTY,
                    STARTS_1IN2_PROPERTY,
                    ENDS_1IN2_PROPERTY,
                )
            ).explode(df_2_column_names_without_groupby_ks).drop_nulls()
        ).sort(grpby_ks)
        # print("bottom_right\n", bottom_right.collect())

        # we cannot horizontally concat a lazy-frame, so we use with_context
        return pl.concat(
            [top_left, bottom_left]
        ).with_context(
            pl.concat([top_right, bottom_right])
        ).select(
            pl.all()
        ).drop([] if self.j.groupby_args_given else self.j.by)

    def overlaps(self):
        grpby_ks = self.j.by
        cols = self.j.colnames_without_groupby_ks()
        top_left = (
            self.data
            .groupby(grpby_ks).agg(
                pl.col(cols).explode().filter(pl.col(MASK_2IN1_PROPERTY).explode()),
            ).explode(cols).drop_nulls()
        ).sort(grpby_ks)

        bottom_left = (
            self.data
            .groupby(grpby_ks).agg(
                self.repeat_other(
                    columns=cols,
                    starts=pl.col(STARTS_1IN2_PROPERTY).explode().filter(
                        ~pl.col(STARTS_1IN2_PROPERTY).explode().is_duplicated()).explode(),
                    diffs=pl.col(LENGTHS_1IN2_PROPERTY).explode().filter(
                        ~pl.col(STARTS_1IN2_PROPERTY).explode().is_duplicated()).explode(),
                )
            ).explode(cols).drop_nulls()
        ).sort(grpby_ks)

        return pl.concat([top_left, bottom_left]).unique(keep="first")
