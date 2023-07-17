import dataclasses
from typing import Optional, Tuple, List, Union

import polars as pl
import poranges.ops


@pl.api.register_lazyframe_namespace("genomics")
@pl.api.register_dataframe_namespace("genomics")
class GenomicsFrame:
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

    def overlaps(
            self,
            other: pl.DataFrame,
            on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            left_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            right_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            by: Optional[List[str]] = None,
            closed_intervals: bool = False
    ) -> "GenomicsFrame":
        starts, ends, starts_2, ends_2 = _get_genomic_coordinate_cols(on, right_on, left_on)

        return poranges.ops.overlaps(
            left=self._df.lazy(),
            right=other.lazy(),
            starts=starts,
            ends=ends,
            starts_2=starts_2,
            ends_2=ends_2,
            by=by,
            closed_intervals=closed_intervals
        )


def _get_genomic_coordinate_cols(
        on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
        left_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
        right_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
) -> "GenomicCoordinateCols":
    if on is None:
        if right_on is None or left_on is None:
            raise ValueError("Either `on` or `right_on` and `left_on` must be specified.")
        if len(right_on) != len(left_on):
            raise ValueError("`right_on` and `left_on` must have the same number of column names.")

        if len(right_on) not in [3, 4]:
            raise ValueError("`right_on` and `left_on` must have 3 or 4 column names.")

    else:
        if len(on) not in [3, 4]:
            raise ValueError("`right_on` and `left_on` must have 3 or 4 column names.")

        starts, ends = on
        starts_2, ends_2 = on

    return starts, ends, starts_2, ends_2


@dataclasses.dataclass
class GenomicCoordinateCols:
    chromosome: str
    starts: str
    ends: str
    chromosome2: str
    starts2: str
    ends2: str
    strand: Optional[str] = None
    strand2: Optional[str] = None
