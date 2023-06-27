import polars as pl

from hypothesis import strategies

from polars.testing.parametric import column, dataframes

dfs = dataframes(
    cols=[
        column("Start", dtype=pl.UInt64, strategy=strategies.integers(min_value=0, max_value=1_000_000)),
        column("lengths", dtype=pl.UInt32, strategy=strategies.integers(min_value=1, max_value=100_000)),
        column("random"),
    ]
)


@strategies.composite
def interval_df(draw):
    df = draw(dfs)
    df = df.select(
        pl.lit("chr1").alias("Chromosome"),
        pl.col("Start"),
        pl.col("Start").add(pl.col("lengths")).alias("End"),
        pl.col("random")
    )
    return df

