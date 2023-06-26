from typing import Optional, Tuple

import polars as pl
import pyoframe


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

        return pyoframe.ops.join(
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

        return pyoframe.ops.overlap(
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

        return pyoframe.ops.closest(
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
