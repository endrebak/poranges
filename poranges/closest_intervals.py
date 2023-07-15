from typing import Literal, TYPE_CHECKING, Optional

import polars as pl

from poranges.constants import DISTANCE_COL_PROPERTY, LEFT_DIRECTION_PROPERTY, RIGHT_DIRECTION_PROPERTY, \
    RIGHT_SIDE_PROPERTY, LENGTHS_2IN1_PROPERTY, CLOSEST_END_IDX_PROPERTY, CLOSEST_START_IDX_PROPERTY, \
    ARANGE_COL_PROPERTY, LEFT_SIDE_PROPERTY, COUNT_PROPERTY
from poranges.ops import search, add_length, arange_multi
from poranges.overlapping_intervals import OverlappingIntervals

if TYPE_CHECKING:
    from poranges.groupby_join_result import GroupByJoinResult


class ClosestIntervals:
    def __init__(
            self,
            j: "GroupByJoinResult",
            closed_intervals: bool = False,
            direction: Literal["any", "left", "right"] = "any",
            k: int = 1,
            distance_col: Optional[str] = None,
            include_overlapping: bool = True,
    ) -> None:
        self.j = j
        self.closed_intervals = closed_intervals
        self.direction = direction
        self.k = k
        self.distance_col_given = distance_col is not None
        self.distance_col = distance_col if self.distance_col_given else DISTANCE_COL_PROPERTY
        self.include_overlapping = include_overlapping

    def closest(self, k: Optional[int] = None) -> pl.LazyFrame:
        k = self.k if k is None else k
        if k > 0 and self.direction == LEFT_DIRECTION_PROPERTY:
            _closest = self.closest_nonoverlapping_left(k=k)
        elif k > 0 and self.direction == RIGHT_DIRECTION_PROPERTY:
            _closest = self.closest_nonoverlapping_right(k=k)
        elif k > 0 and self.direction == "any":
            _left = self.closest_nonoverlapping_left(k=k)
            _right = self.closest_nonoverlapping_right(k=k)
            print("LEFT\n", _left.collect())
            print("RIGHT\n", _right.collect())
            _closest = pl.concat([_left, _right])
            print("AFTER")
        else:
            raise ValueError("`direction` must be one of 'left', 'right', or 'any'")

        if self.include_overlapping:
            overlaps = OverlappingIntervals(self.j, self.closed_intervals).overlapping_pairs().with_columns(
                    pl.lit(0, dtype=pl.UInt64).alias(self.distance_col)
                )
            print("overlaps")
            print(overlaps.collect())
            print("closest")
            print(_closest.collect())
            print(self.j.columns)
            _k_closest = pl.concat([overlaps, _closest]).sort(self.distance_col).groupby(self.j.columns).agg(
                pl.all().head(k)
            ).explode(pl.exclude(self.j.columns)).select(
                pl.exclude(COUNT_PROPERTY).repeat_by(pl.col(COUNT_PROPERTY)).explode()
            )
            print("k ", k)
            print("k_closest")
            print(_k_closest.collect())
            if not self.distance_col_given:
                _k_closest = _k_closest.drop(self.distance_col)
        else:
            _k_closest = _closest
        return _k_closest

    def closest_nonoverlapping_left(self, k: Optional[int] = None) -> pl.LazyFrame:
        k = self.k if k is None else k
        sorted_on_other_end = self.j.joined.groupby(self.j.by).agg(
            [
                pl.exclude(self.j.ends_2_renamed),
                pl.col(self.j.ends_2_renamed).explode()
            ]
        ).explode(self.j.ends_2_renamed).sort(self.j.ends_2_renamed).groupby(self.j.by).agg(
            [
                pl.exclude(self.j.ends_2_renamed).first().explode(),
                pl.col(self.j.ends_2_renamed)
            ]
        )
        res = (
            sorted_on_other_end.groupby(self.j.by).agg(
                pl.all().explode()
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    search(self.j.ends_2_renamed, self.j.starts, side=RIGHT_SIDE_PROPERTY)
                    .cast(pl.Int64)
                    .alias(CLOSEST_END_IDX_PROPERTY),
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    pl.max_horizontal([pl.col(CLOSEST_END_IDX_PROPERTY).explode() - k, pl.lit(0)])
                    .alias(CLOSEST_START_IDX_PROPERTY)
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    add_length(
                        starts=CLOSEST_START_IDX_PROPERTY,
                        ends=CLOSEST_END_IDX_PROPERTY,
                        alias=LENGTHS_2IN1_PROPERTY,
                    ).explode()
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    arange_multi(
                        starts=pl.col(CLOSEST_START_IDX_PROPERTY).explode().filter(pl.col(LENGTHS_2IN1_PROPERTY).explode().gt(0)),
                        diffs=pl.col(LENGTHS_2IN1_PROPERTY).explode().filter(pl.col(LENGTHS_2IN1_PROPERTY).explode().gt(0))
                    ).alias(ARANGE_COL_PROPERTY)
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.col(self.j.colnames_without_groupby_ks()).explode().repeat_by(pl.col(LENGTHS_2IN1_PROPERTY).explode()).explode().drop_nulls(),
                    pl.col(self.j.colnames_df2_after_join())
                    .explode()
                    .take(pl.col(ARANGE_COL_PROPERTY).explode().cast(pl.UInt32))
                ]
            ).filter(pl.col(self.j.starts).list.lengths().gt(0))
        )
        print(res.collect())

        res = res.groupby(self.j.by).agg(
            [
                pl.exclude(self.j.by).explode(),
                pl.col(self.j.starts).explode().sub(
                    pl.col(self.j.ends_2_renamed).explode()
                ).add(1).cast(pl.UInt64).alias(self.distance_col)
            ]
        )

        return res.select(
            [
                pl.col(self.j.by).repeat_by(pl.col(self.j.starts).list.lengths().explode()).explode(),
                pl.exclude(self.j.by).explode()
            ]
        )

    def closest_nonoverlapping_right(self, k: Optional[int] = None) -> pl.LazyFrame:
        k = self.k if k is None else k
        res = (
            self.j.joined.groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    search(self.j.starts_2_renamed, self.j.ends, side=LEFT_SIDE_PROPERTY)
                    .cast(pl.Int64)
                    .alias(CLOSEST_START_IDX_PROPERTY),
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    pl.min_horizontal([pl.col(CLOSEST_START_IDX_PROPERTY).explode() + k, pl.col(self.j.starts_2_renamed).explode().len()])
                    .alias(CLOSEST_END_IDX_PROPERTY)
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    add_length(
                        starts=CLOSEST_START_IDX_PROPERTY,
                        ends=CLOSEST_END_IDX_PROPERTY,
                        alias=LENGTHS_2IN1_PROPERTY
                    ).explode()
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.all().explode(),
                    arange_multi(
                        starts=pl.col(CLOSEST_START_IDX_PROPERTY).explode(),
                        diffs=pl.col(LENGTHS_2IN1_PROPERTY).explode(),
                    ).alias(ARANGE_COL_PROPERTY)
                ]
            )
            .groupby(self.j.by).agg(
                [
                    pl.col(self.j.colnames_without_groupby_ks()).explode().repeat_by(pl.col(LENGTHS_2IN1_PROPERTY).explode()).explode().drop_nulls(),
                    pl.col(self.j.colnames_df2_after_join()).explode().take(pl.col(ARANGE_COL_PROPERTY).explode().cast(pl.UInt32))
                ]
            )
        ).filter(pl.col(self.j.starts).list.lengths().gt(0))

        res = res.groupby(self.j.by).agg(
                [
                pl.all().explode(),
                pl.col(self.j.starts_2_renamed).explode().sub(
                    pl.col(self.j.ends).explode()
                ).add(pl.lit(1)).cast(pl.UInt64).alias(self.distance_col)
            ]
        )
        return res.select(
            [
                pl.col(self.j.by).repeat_by(pl.col(self.j.starts).list.lengths().explode()).explode(),
                pl.exclude(self.j.by).explode()
            ]
        )
