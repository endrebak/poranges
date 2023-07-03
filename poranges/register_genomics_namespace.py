from typing import Optional, Tuple, List

import polars as pl
import poranges.ops


@pl.api.register_genomics_namespace("genomics")
class IntervalFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def merge(
            self: pl.DataFrame,
            chromosome: str,
            starts: str,
            ends: str,
            by: Optional[List[str]] = None,
            merge_bookended: bool = False,
            keep_original_columns: bool = True,
            min_distance: int = 0,
            suffix: str = "_before_merge"
    ):
        return poranges.ops.merge(
            df=self._df.lazy(),
            starts=starts,
            ends=ends,
            by=[chromosome] + by if by is not None else [chromosome],
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
