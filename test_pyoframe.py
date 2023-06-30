from typing import List
import polars as pl

df = pl.DataFrame(
    {
        "a": [[1, 2, 3], [8, 9]],
        "b": [[2], [6, 10]]
    }
)
print(df)

# print(df.explode("a"))
# df2 = pl.DataFrame(
#     {
#     "a": [8, 9],
#     }
# )
#
# print(df2["a"].search_sorted([10, 6]))

# res = df.lazy().with_row_count().groupby("row_nr").agg(
#     [
#         pl.all().exclude("row_nr").explode(),
#         pl.col("a").explode().search_sorted(pl.col("b").explode(), side="left").alias("c"),
#         pl.col("b").explode().search_sorted(pl.col("a").explode(), side="left").alias("d"),
#         pl.col("a").explode().search_sorted(pl.col("b").explode(), side="right").alias("e"),
#     ]
# )

# Should ideally filter within the lazy computation

#     select(
#     [
#         pl.col("a").list.eval(pl.element().search_sorted(pl.col("b"), side="left")).implode().alias("c")
#     ]
# )

# print(res)
# print(res.explain(optimized=True))
# print(res.collect())

# import poranges as pf
#
df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [0, 8, 6, 5],
        "ends": [6, 9, 10, 7]
    }
)

df2 = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1"],
        "starts": [6, 3, 1],
        "ends": [7, 8, 2],
    }
)

print(df)
print(df2)

g = df.lazy().groupby("chromosome").all().join(df2.lazy().groupby("chromosome").all(), on="chromosome", suffix="_2")


def search(
    col1: str,
    col2: str,
    side: str = "left"
) -> pl.Expr:
    return pl.col(col1).explode().search_sorted(pl.col(col2).explode(), side=side)


def lengths(
    starts: str,
    ends: str,
    outname: str = ""
) -> pl.Expr:
    return pl.col(ends).explode().sub(pl.col(starts).explode()).explode().alias(outname)


def find_starts_in_ends():
    return [
        pl.all().exclude("chromosome_2").explode(),
        search("starts_2", "starts").alias("starts_2in1"),
        search("starts_2", "ends").alias("ends_2in1"),
        search("starts", "starts_2", side="right").alias("starts_1in2"),
        search("starts", "ends_2").alias("ends_1in2"),
    ]

print(g.groupby("chromosome").agg(find_starts_in_ends()).collect())
raise


res2 = g.groupby("chromosome").agg(
    find_starts_in_ends()
).groupby("chromosome").agg(
    [
        pl.all().explode(),
        pl.col("ends_1in2").explode().gt(pl.col("starts_1in2").explode()).alias("mask_1in2"),
        pl.col("ends_2in1").explode().gt(pl.col("starts_2in1").explode()).alias("mask_2in1"),
     ]
).groupby("chromosome").agg(
    [
        pl.exclude("mask_2in1", "mask_1in2", "ends_2in1", "starts_2in1", "ends_1in2", "starts_1in2", "starts", "ends", "starts_2", "ends_2").explode(),
        pl.col(["ends_2in1", "starts_2in1"]).explode().filter(pl.col("mask_2in1").explode()).explode(),
        pl.col(["ends_1in2", "starts_1in2"]).explode().filter(pl.col("mask_1in2").explode()).explode(),
        pl.col(["starts", "ends"]).explode().filter(pl.col("mask_2in1").explode()).explode(),
        pl.col(["starts_2", "ends_2"]).explode().filter(pl.col("mask_1in2").explode()).explode(),
        # pl.col(["starts_2in1", "ends_2in1"] + df.columns).explode()# .filter(pl.col("mask_2in1").explode()).map_alias(lambda s: s + "_b"),
        # pl.col("starts_1in2", "ends_1in2").explode().filter(pl.col("mask_1in2").explode())
    ]
).groupby("chromosome").agg(
 [
     pl.all().explode(),
     lengths("starts", "ends", "lengths"),
     # pl.col("starts").explode().repeat_by(lengths("starts", "ends")).alias("starts_repeated"),
     # lengths("starts", "ends").sum().alias("total_length"),
     # lengths("starts_2", "ends_2", "lengths_2"),
     # pl.col("starts_2").explode().repeat_by(lengths("starts_2", "ends_2")).alias("starts_repeated_2"),
     # lengths("starts_2", "ends_2").sum().alias("total_length_2"),
 ]
)#.groupby("chromosome").agg(
 #   [
 #       pl.all().explode(),
 #       pl.col("lengths").explode().cumsum().sub(pl.col("lengths").explode()).repeat_by(pl.col("lengths").explode()).add(pl.col("starts_repeated").explode()).alias("test"),
 #   ]
# )

#.with_columns(
#     [
#         pl.col("ends_2in1", "starts_2in1").filter(pl.col("mask_2in1").explode()).implode(),
#     ]
# )
print(df.columns)

with pl.Config() as cfg:
    cfg.set_tbl_cols(-1)
    print(res2.collect())

#
# print(df)
# print(df2)
#
# g = df.lazy().groupby("chromosome").all().join(df2.lazy().groupby("chromosome").all(), on="chromosome")
# print(g)
# print(g.collect().to_dict())
# res = g.with_columns(
# [
#     pl.col("starts", "starts_right").list.eval(pl.element().search_sorted(pl.element()))
# ]
# )
# print(res.collect())

# df = pl.DataFrame(
# {
#     "chromosome": ["chr1", "chr2", "chr2"],
#     "starts1": [0, 8, 6],
#     "ends1": [6, 9, 10],
#     "genes": ["a", "b", "c"]
# }
# )
# df2 = pl.DataFrame(
# {
#     "chromosome": ["chr1", "chr1", "chr2"],
#     "starts2": [6, 3, 1],
#     "ends2": [7, 8, 2],
# }
# )
# c = df.lazy().with_context(df2.lazy())

## j = pf.overlaps(df, df2)
## print(j)
## 
## # shape: (6, 6)
## # ┌────────────┬─────────┬───────┬───────┬─────────┬───────┐
## # │ chromosome ┆ starts1 ┆ ends1 ┆ genes ┆ starts2 ┆ ends2 │
## # │ ---        ┆ ---     ┆ ---   ┆ ---   ┆ ---     ┆ ---   │
## # │ str        ┆ i64     ┆ i64   ┆ str   ┆ i64     ┆ i64   │
## # ╞════════════╪═════════╪═══════╪═══════╪═════════╪═══════╡
## # │ chr1       ┆ 0       ┆ 6     ┆ a     ┆ 1       ┆ 2     │
## # │ chr1       ┆ 0       ┆ 6     ┆ a     ┆ 3       ┆ 8     │
## # │ chr1       ┆ 5       ┆ 7     ┆ d     ┆ 6       ┆ 7     │
## # │ chr1       ┆ 6       ┆ 10    ┆ c     ┆ 6       ┆ 7     │
## # │ chr1       ┆ 5       ┆ 7     ┆ d     ┆ 3       ┆ 8     │
## # │ chr1       ┆ 6       ┆ 10    ┆ c     ┆ 3       ┆ 8     │
## # └────────────┴─────────┴───────┴───────┴─────────┴───────┘
## 
## 
## 

cat_start [0 0 2 2]
lengths.sum() 4
lengths.cumsum() - lengths [0 2 3]
np.repeat(lengths.cumsum() - lengths, lengths) [0 0 2 3]
cat_counter [0 1 0 0]