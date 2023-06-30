import polars as pl
df = pl.DataFrame({"starts": [0], "ends": [1]})
df2 = pl.DataFrame({"starts": [0], "ends": [1]})
import poranges as pf
print(df.interval.join(df2, on=("starts", "ends")).collect())

raise
import polars as pl

df = pl.DataFrame({"starts": [0, 80, 106, 5], "ends": [60, 290, 200, 107]})

raise

df = pl.DataFrame(
    {
        "chromosome": "chr1",
        "top_right": [[0]],
        "top_left": [[3]],
        "bottom_left": [[6, 7]],
        "bottom_right": [[4, 5]],
    }
)

df.groupby("chromosome").agg(
    pl.concat(
        [
            pl.col("top_left", "top_right").alias_map(lambda n: n.split("_")[1]),
            pl.col("bottom_left", "bottom_right").alias_map(lambda n: n.split("_")[1]),
        ],
        how="horizontal",
    )
)

raise


# not so simple
# cannot just remove all but first tiles
df2 = pl.DataFrame({"starts": [32], "ends": [490]})

win_size = 100


def to_window(column):
    return pl.col(column).sub(pl.col(column).mod(pl.lit(win_size)))


def all_overlapping_windows_list(start_col, end_col):
    return pl.arange(to_window(start_col), to_window(end_col) + 1, win_size)


def one_row_per_overlapping_window(window_list_col):
    return [
        pl.exclude(window_list_col)
        .repeat_by(pl.col(window_list_col).list.lengths())
        .explode(),
        pl.col(window_list_col).explode(),
    ]


print(
    df.lazy()
    .sort("starts", "ends")
    .with_columns([all_overlapping_windows_list("starts", "ends").alias("windows")])
    .select(one_row_per_overlapping_window("windows"))
    .sort("windows")
    .collect()
)


# print(g.groupby(pl.col("arange")).agg([pl.lit(1).alias("g")]).collect()
