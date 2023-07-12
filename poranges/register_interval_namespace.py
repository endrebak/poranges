from typing import Optional, Tuple, List

import polars as pl
import poranges.ops
from poranges.closest_intervals import ClosestIntervals
from poranges.constants import DUMMY_SUFFIX_PROPERTY
from poranges.overlapping_intervals import OverlappingIntervals
from poranges.groupby_join_result import GroupByJoinResult


@pl.api.register_dataframe_namespace("interval")
class IntervalFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def join(
            self,
            other: pl.DataFrame,
            on: Optional[Tuple[str, str]] = None,
            right_on: Optional[Tuple[str, str]] = None,
            left_on: Optional[Tuple[str, str]] = None,
            suffix: str = "_right",
            by: Optional[List[str]] = None,
            closed_intervals: bool = False
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        j = GroupByJoinResult(self._df.lazy(), other.lazy(), starts, ends, starts_2, ends_2, suffix, by)
        if j.empty():
            result = j.joined
        else:
            result = OverlappingIntervals(
                j=j,
                closed_intervals=closed_intervals
            ).overlaps()

        return result.drop([] if j.groupby_args_given else j.by)

    def overlap(
            self,
            other: pl.DataFrame,
            on: Optional[Tuple[str, str]] = None,
            right_on: Optional[Tuple[str, str]] = None,
            left_on: Optional[Tuple[str, str]] = None,
            by: Optional[List[str]] = None,
            closed_intervals: bool = False
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        j = GroupByJoinResult(self._df.lazy(), other.lazy(), starts, ends, starts_2, ends_2, DUMMY_SUFFIX_PROPERTY, by)
        if j.empty():
            result = j.joined
        else:
            result = OverlappingIntervals(
                j=j,
                closed_intervals=closed_intervals
            ).overlaps()

        return result.drop([] if j.groupby_args_given else j.by)

    def closest(
            self,
            other: pl.DataFrame,
            *,
            on: Optional[Tuple[str, str]] = None,
            right_on: Optional[Tuple[str, str]] = None,
            left_on: Optional[Tuple[str, str]] = None,
            suffix: str = "_right",
            k: int = 1,
            distance_col: Optional[str] = None,
            by: Optional[List[str]] = None
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        j = GroupByJoinResult(df=self._df.lazy(), df2=other.lazy(), starts=starts, ends=ends, starts_2=starts_2, ends_2=ends_2, suffix=suffix, by=by)
        if j.empty():
            result = pl.LazyFrame(schema=j.joined.schema)
        else:
            result = ClosestIntervals(
                j=j,
                k=k,
                distance_col=distance_col
            ).closest()

        print(result.collect())

        print(j.groupby_args_given, j.by)
        return result.drop([] if j.groupby_args_given else j.by)

    def merge(
            self: pl.DataFrame,
            starts: str,
            ends: str,
            merge_bookended: bool = False,
            keep_original_columns: bool = True,
            min_distance: int = 0,
            suffix: str = "_before_merge",
            by: Optional[List[str]] = None
    ):
        return poranges.ops.merge(
            df=self._df.lazy(),
            starts=starts,
            ends=ends,
            merge_bookended=merge_bookended,
            keep_original_columns=keep_original_columns,
            min_distance=min_distance,
            suffix=suffix,
            by=by
        )


def _get_interval_columns(
        on: Optional[Tuple[str, str]] = None,
        right_on: Optional[Tuple[str, str]] = None,
        left_on: Optional[Tuple[str, str]] = None,
) -> Tuple[str, str, str, str]:
    if on is None:
        if right_on is None or left_on is None:
            raise ValueError(
                "Either `on` or `right_on` and `left_on` must be specified."
            )
        starts, ends = left_on
        starts_2, ends_2 = right_on
    else:
        starts, ends = on
        starts_2, ends_2 = on

    return starts, ends, starts_2, ends_2
