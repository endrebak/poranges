from typing import Optional, List

import polars as pl

from poranges.constants import ROW_NUMBER_PROPERTY


class GroupByJoinResult:
    def __init__(
            self,
            df: pl.LazyFrame,
            df2: pl.LazyFrame,
            starts: str,
            ends: str,
            starts_2: str,
            ends_2: str,
            suffix: str,
            by: Optional[List[str]] = None
    ):
        self.starts_2_renamed = starts + suffix if starts == starts_2 else starts_2
        self.ends_2_renamed = ends + suffix if ends == ends_2 else ends_2
        self.df = df
        self.df2 = df2
        self.starts = starts
        self.ends = ends
        self.suffix = suffix
        self.groupby_args_given = by is not None
        self.by = by if self.groupby_args_given else [ROW_NUMBER_PROPERTY]

        if by is None:
            sorted_collapsed = df.sort(starts, ends).select(pl.all().implode())
            sorted_collapsed_2 = df2.sort(starts_2, ends_2).select(pl.all().implode())
            self.joined = sorted_collapsed.join(
                sorted_collapsed_2, how="cross", suffix=suffix
            ).with_row_count(ROW_NUMBER_PROPERTY)
        else:
            sorted_collapsed = df.sort(starts, ends).groupby(self.by).all()
            sorted_collapsed_2 = df2.sort(starts_2, ends_2).groupby(self.by).all()
            self.joined = sorted_collapsed.join(sorted_collapsed_2, on=self.by, suffix=suffix)

    def empty(self) -> bool:
        at_least_one_df_empty = self.df.first().collect().shape[0] == 0 or self.df2.first().collect().shape[0] == 0
        # return nothing if one df is nonempty or j is nonempty.
        # this is to avoid having to make the downstream code more complicated by checking for empty data
        return at_least_one_df_empty or self.joined.first().collect().shape[0] == 0

    def colnames_df2_after_join(self) -> List[str]:
        possibly_duplicated_cols = self.df2.columns if self.by is None else [c for c in self.df2.columns if c not in self.by]
        return [
            col2 + self.suffix if col2 in set(self.df.columns) else col2 for col2 in possibly_duplicated_cols
        ]

    def colnames_without_groupby_ks(self) -> List[str]:
        return [c for c in self.df.columns if c not in self.by]

    def colnames_2_without_groupby_ks(self) -> List[str]:
        return [c for c in self.colnames_df2_after_join() if c not in self.by]
