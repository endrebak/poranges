import pandas as pd
import polars as pl
import pyranges as pr


def to_pyranges(df):
    return pr.PyRanges(df.to_pandas())


def compare_frames(*, pl_df: pl.DataFrame, pd_df: pd.DataFrame) -> None:
    pl_df = pl_df.select(
        pl.exclude("^*Chromosome*$"),
    ).to_pandas()
    if pl_df.empty and pd_df.empty:
        return

    comparison_cols = ["Start", "End", "Start_right", "End_right"]
    pd_df = pd_df.sort_values(by=comparison_cols)
    pl_df = pd_df.sort_values(by=comparison_cols)

    for col in comparison_cols:
        pl_df_col_ = list(pl_df[col])
        pd_df_col_ = list(pd_df[col])
        assert pl_df_col_ == pd_df_col_

