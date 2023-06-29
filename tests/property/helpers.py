from typing import Tuple

import pandas as pd
import polars as pl
import pyranges as pr


def to_pyranges(df):
    return pr.PyRanges(df.to_pandas())


def compare_frames(
        *,
        pl_df: pl.DataFrame,
        pd_df: pd.DataFrame,
        comparison_cols: Tuple[str, ...] = ("Start", "End", "Start_right", "End_right")
) -> None:
    pl_df = pl_df.select(
        pl.exclude("^*Chromosome*$"),
    ).to_pandas()
    if pl_df.empty and pd_df.empty:
        return

    pd_df = pd_df.sort_values(by=comparison_cols) if not pd_df.empty else pd_df
    pl_df = pl_df.sort_values(by=comparison_cols) if not pl_df.empty else pl_df

    print("before" * 50)
    for col in comparison_cols:
        print("col" * 50, col)
        pl_df_col_ = list(pl_df[col])
        pd_df_col_ = list(pd_df[col])
        assert pl_df_col_ == pd_df_col_

