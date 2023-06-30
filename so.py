import polars as pl

df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1", "chr2", "chr2"],
        "starts": [0, 8, 6, 5, 0, 10],
        "ends": [6, 9, 10, 7, 11, 20],
        "genes": ["a", "b", "c", "d", "A", "B"],
    }
).sort("starts", "ends")


def search(col1: str, col2: str, side: str = "left") -> pl.Expr:
    return pl.col(col1).explode().search_sorted(pl.col(col2).explode(), side=side)


res = (
    df.lazy()
    .groupby("chromosome")
    .agg(
        [
            search("starts", "ends")
            .alias("ends_in_starts")
            .gt(search("ends", "starts").alias("starts_in_ends"))
            .alias("mask"),
        ]
    )
)

res = (
    df.lazy()
    .groupby("chromosome")
    .agg(
        [
            search("starts", "ends").gt(search("ends", "starts")).alias("mask"),
            search("starts", "ends").alias("ends_in_starts"),
            search("ends", "starts").alias("starts_in_ends"),
        ]
    )
)

print(res.explain(optimized=True) == res.explain(optimized=False))

print(res.explain(optimized=True))
print(res.explain(optimized=False))
print(res.collect())
