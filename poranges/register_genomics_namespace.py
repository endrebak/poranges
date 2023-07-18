import dataclasses
from typing import Optional, Tuple, List, Union, Literal

import polars as pl
import poranges.ops
from poranges.constants import DUMMY_SUFFIX_PROPERTY
from poranges.groupby_join_result import GroupByJoinResult
from poranges.overlapping_intervals import OverlappingIntervals


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

    def overlap(
            self,
            other: pl.DataFrame,
            on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            left_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            right_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            by: Optional[List[str]] = None,
            closed_intervals: bool = False,
            strand_join: Literal["same", "opposite", "any"] = "any"
    ) -> "GenomicsFrame":
        print(self)

        print(other)
        coordinate_cols = GenomicCoordinateCols.from_ons(on, right_on, left_on)
        print(coordinate_cols.strand_2_renamed(DUMMY_SUFFIX_PROPERTY))

        if strand_join in ["same", "opposite"]:
            if coordinate_cols.both_have_strands:
                other = other.with_columns(
                    pl.col(coordinate_cols.strand_2).map_dict(
                        remapping={"+": "-", "-": "+"},
                        default=".",
                    ).keep_name(),
                )
            else:
                raise ValueError(
                    "Strand join is set to 'same' or 'opposite', but not both dataframes have strand columns."
                )

        j = GroupByJoinResult(
            self._df.lazy(),
            other.lazy(),
            starts=coordinate_cols.starts,
            ends=coordinate_cols.ends,
            starts_2=coordinate_cols.starts_2,
            ends_2=coordinate_cols.ends_2,
            suffix=DUMMY_SUFFIX_PROPERTY,
            by=coordinate_cols.by_columns() + by if by is not None else coordinate_cols.by_columns(),
        )
        print(j.joined.collect())
        if j.empty():
            result = j.joined
        else:
            result = OverlappingIntervals(
                j=j,
                closed_intervals=closed_intervals
            ).overlaps()

        if strand_join == "opposite":
            result = result.with_columns(
                pl.col(coordinate_cols.strand_2_renamed(DUMMY_SUFFIX_PROPERTY)).map_dict(
                    remapping={"+": "-", "-": "+"},
                    default=".",
                ).keep_name(),
            )

        return result.drop([] if j.groupby_args_given else j.by)


def _extract_from_tuple(
        on: Union[Tuple[str, str, str], Tuple[str, str, str, str]],
) -> Tuple[str, str, str, str]:
    if len(on) == 3:
        on = (*on, None)
    return on


@dataclasses.dataclass
class GenomicCoordinateCols:
    chromosome: str
    starts: str
    ends: str
    chromosome_2: str
    starts_2: str
    ends_2: str
    strand: Optional[str] = None
    strand_2: Optional[str] = None

    def strand_2_renamed(self, suffix: str) -> str:
        return f"{self.strand_2}{suffix}" if self.strand_2 == self.strand else self.strand_2

    @property
    def both_have_strands(self) -> bool:
        return self.strand is not None and self.strand_2 is not None

    def by_columns(self) -> List[str]:
        return [self.chromosome, self.strand] if self.strand is not None else [self.chromosome]

    @staticmethod
    def from_ons(
            on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            left_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
            right_on: Optional[Union[Tuple[str, str, str], Tuple[str, str, str, str]]] = None,
    ) -> "GenomicCoordinateCols":
        if on is None:
            if right_on is None or left_on is None:
                raise ValueError("Either `on` or `right_on` and `left_on` must be specified.")
            return GenomicCoordinateCols._from_left_right_on(left_on=left_on, right_on=right_on)
        else:
            return GenomicCoordinateCols._from_on(on=on)

    @staticmethod
    def _from_on(
            on: Union[Tuple[str, str, str], Tuple[str, str, str, str]],
    ) -> "GenomicCoordinateCols":
        chromosomes, starts, ends, strand = _extract_from_tuple(on)
        if len(on) not in [3, 4]:
            raise ValueError("`on` must have 3 or 4 column names.")
        return GenomicCoordinateCols(
            chromosome=chromosomes,
            starts=starts,
            ends=ends,
            strand=strand,
            chromosome_2=chromosomes,
            starts_2=starts,
            ends_2=ends,
            strand_2=strand
        )

    @staticmethod
    def _from_left_right_on(
            left_on: Union[Tuple[str, str, str], Tuple[str, str, str, str]],
            right_on: Union[Tuple[str, str, str], Tuple[str, str, str, str]],
    ) -> "GenomicCoordinateCols":
        if len(right_on) != len(left_on):
            raise ValueError("`right_on` and `left_on` must have the same number of column names.")

        if len(right_on) not in [3, 4]:
            raise ValueError("`right_on` and `left_on` must have 3 or 4 column names.")

        chromosomes, starts, ends, strand = _extract_from_tuple(left_on)
        chromosomes_2, starts_2, ends_2, strand_2 = _extract_from_tuple(right_on)

        return GenomicCoordinateCols(
            chromosome=chromosomes,
            starts=starts,
            ends=ends,
            strand=strand,
            chromosome_2=chromosomes_2,
            starts_2=starts_2,
            ends_2=ends_2,
            strand_2=strand_2
        )

