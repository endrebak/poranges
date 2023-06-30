from typing import Optional, Tuple

import polars as pl
import poranges.ops


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
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        return poranges.ops.join(
            self._df.lazy(),
            other.lazy(),
            starts=starts,
            ends=ends,
            starts_2=starts_2,
            ends_2=ends_2,
            suffix=suffix,
        )

    def overlap(
            self,
            other: pl.DataFrame,
            on: Optional[Tuple[str, str]] = None,
            right_on: Optional[Tuple[str, str]] = None,
            left_on: Optional[Tuple[str, str]] = None,
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        return poranges.ops.overlap(
            self._df.lazy(),
            other.lazy(),
            starts=starts,
            ends=ends,
            starts_2=starts_2,
            ends_2=ends_2,
        )

    def closest(
            self,
            other: pl.DataFrame,
            *,
            on: Optional[Tuple[str, str]] = None,
            right_on: Optional[Tuple[str, str]] = None,
            left_on: Optional[Tuple[str, str]] = None,
            suffix: str = "_right",
            k: int = 1,
            distance_col: Optional[str] = None
    ):
        starts, ends, starts_2, ends_2 = _get_interval_columns(on, right_on, left_on)

        return poranges.ops.closest(
            self._df.lazy(),
            other.lazy(),
            starts=starts,
            ends=ends,
            starts_2=starts_2,
            ends_2=ends_2,
            suffix=suffix,
            k=k,
            distance_col=distance_col
        )

    def merge(
            self: pl.DataFrame,
            starts: str,
            ends: str,
            merge_bookended: bool = False,
            keep_original_columns: bool = True,
            min_distance: int = 0,
            suffix: str = "_before_merge"
    ):
        return poranges.ops.merge(
            df=self._df.lazy(),
            starts=starts,
            ends=ends,
            merge_bookended=merge_bookended,
            keep_original_columns=keep_original_columns,
            min_distance=min_distance,
            suffix=suffix
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
