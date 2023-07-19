# poranges

## This project is not ready for public consumption yet, but a beta will be out soon.

Interval operations for polars.

Interval operations can be used for any frame where the start and end columns representing the intervals are orderable and of the same type.

The currently supported binary operations are join, k-closest, and overlap.
The currently supported unary operations are merge and cluster.

This is a work in progress and is accepting contributions.

# Examples (genome aware)

Genome aware interval operations are found in the genome namespace.

This namespace currently only implements the merge/cluster operation.

## Merge

```python
import polars as pl
import poranges.register_genomics_namespace

df = pl.DataFrame(
    {
        "chrom": [1, 2, 1, 1],
        "starts": [4, 10, 7, 1],
        "ends": [5, 11, 8, 6],
        "weights": [-42, 0, 2, 1],
    }
)
# shape: (4, 4)
# ┌───────┬────────┬──────┬─────────┐
# │ chrom ┆ starts ┆ ends ┆ weights │
# │ ---   ┆ ---    ┆ ---  ┆ ---     │
# │ i64   ┆ i64    ┆ i64  ┆ i64     │
# ╞═══════╪════════╪══════╪═════════╡
# │ 1     ┆ 4      ┆ 5    ┆ -42     │
# │ 2     ┆ 10     ┆ 11   ┆ 0       │
# │ 1     ┆ 7      ┆ 8    ┆ 2       │
# │ 1     ┆ 1      ┆ 6    ┆ 1       │
# └───────┴────────┴──────┴─────────┘

df.genomics.merge()
# shape: (3, 6)
# ┌───────┬────────┬──────┬─────────────────────┬───────────────────┬───────────┐
# │ chrom ┆ starts ┆ ends ┆ starts_before_merge ┆ ends_before_merge ┆ weights   │
# │ ---   ┆ ---    ┆ ---  ┆ ---                 ┆ ---               ┆ ---       │
# │ i64   ┆ i64    ┆ i64  ┆ list[i64]           ┆ list[i64]         ┆ list[i64] │
# ╞═══════╪════════╪══════╪═════════════════════╪═══════════════════╪═══════════╡
# │ 2     ┆ 10     ┆ 11   ┆ [10]                ┆ [11]              ┆ [0]       │
# │ 1     ┆ 1      ┆ 6    ┆ [1, 4]              ┆ [6, 5]            ┆ [1, -42]  │
# │ 1     ┆ 7      ┆ 8    ┆ [7]                 ┆ [8]               ┆ [2]       │
# └───────┴────────┴──────┴─────────────────────┴───────────────────┴───────────┘
```


# Examples (genome agnostic)

General interval operations are found in the interval namespace.

## Join

```python
from datetime import date
import polars as pl
import poranges.register_interval_namespace

df_1 = pl.DataFrame(
    {
        "id": ["1", "3", "2"],
        "start": [date(2022, 1, 1), date(2022, 5, 11), date(2022, 3, 4), ],
        "end": [date(2022, 2, 4), date(2022, 5, 16), date(2022, 3, 10), ],
    }
)
# shape: (3, 3)
# ┌─────┬────────────┬────────────┐
# │ id  ┆ start      ┆ end        │
# │ --- ┆ ---        ┆ ---        │
# │ str ┆ date       ┆ date       │
# ╞═════╪════════════╪════════════╡
# │ 1   ┆ 2022-01-01 ┆ 2022-02-04 │
# │ 3   ┆ 2022-05-11 ┆ 2022-05-16 │
# │ 2   ┆ 2022-03-04 ┆ 2022-03-10 │
# └─────┴────────────┴────────────┘

df_2 = pl.DataFrame(
    {
        "start": [date(2021, 12, 31), date(2025, 12, 31), ],
        "end": [date(2022, 4, 1), date(2025, 4, 1), ],
    }
)
# shape: (2, 2)
# ┌────────────┬────────────┐
# │ start      ┆ end        │
# │ ---        ┆ ---        │
# │ date       ┆ date       │
# ╞════════════╪════════════╡
# │ 2021-12-31 ┆ 2022-04-01 │
# │ 2025-12-31 ┆ 2025-04-01 │
# └────────────┴────────────┘

df_1.interval.join(df_2, on=("start", "end"), suffix="_whatevz")
# shape: (2, 5)
# ┌─────┬────────────┬────────────┬───────────────┬─────────────┐
# │ id  ┆ start      ┆ end        ┆ start_whatevz ┆ end_whatevz │
# │ --- ┆ ---        ┆ ---        ┆ ---           ┆ ---         │
# │ str ┆ date       ┆ date       ┆ date          ┆ date        │
# ╞═════╪════════════╪════════════╪═══════════════╪═════════════╡
# │ 1   ┆ 2022-01-01 ┆ 2022-02-04 ┆ 2021-12-31    ┆ 2022-04-01  │
# │ 2   ┆ 2022-03-04 ┆ 2022-03-10 ┆ 2021-12-31    ┆ 2022-04-01  │
# └─────┴────────────┴────────────┴───────────────┴─────────────┘
```

## Closest

```python
import polars as pl
import poranges as po

df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [4, 1, 10, 7],
        "ends": [5, 4, 11, 8],
    }
)
# shape: (4, 3)
# ┌────────────┬────────┬──────┐
# │ chromosome ┆ starts ┆ ends │
# │ ---        ┆ ---    ┆ ---  │
# │ str        ┆ i64    ┆ i64  │
# ╞════════════╪════════╪══════╡
# │ chr1       ┆ 4      ┆ 5    │
# │ chr1       ┆ 1      ┆ 4    │
# │ chr1       ┆ 10     ┆ 11   │
# │ chr1       ┆ 7      ┆ 8    │
# └────────────┴────────┴──────┘

df2 = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [5, 0, 8, 6],
        "ends": [7, 2, 9, 10],
    }
)
# shape: (4, 3)
# ┌────────────┬────────┬──────┐
# │ chromosome ┆ starts ┆ ends │
# │ ---        ┆ ---    ┆ ---  │
# │ str        ┆ i64    ┆ i64  │
# ╞════════════╪════════╪══════╡
# │ chr1       ┆ 5      ┆ 7    │
# │ chr1       ┆ 0      ┆ 2    │
# │ chr1       ┆ 8      ┆ 9    │
# │ chr1       ┆ 6      ┆ 10   │
# └────────────┴────────┴──────┘

df.interval.closest(df2, on=("starts", "ends"), k=2, distance_col="distance")
# shape: (8, 7)
# ┌────────────┬────────┬──────┬──────────────────┬──────────────┬────────────┬──────────┐
# │ chromosome ┆ starts ┆ ends ┆ chromosome_right ┆ starts_right ┆ ends_right ┆ distance │
# │ ---        ┆ ---    ┆ ---  ┆ ---              ┆ ---          ┆ ---        ┆ ---      │
# │ str        ┆ i64    ┆ i64  ┆ str              ┆ i64          ┆ i64        ┆ u64      │
# ╞════════════╪════════╪══════╪══════════════════╪══════════════╪════════════╪══════════╡
# │ chr1       ┆ 1      ┆ 4    ┆ chr1             ┆ 0            ┆ 2          ┆ 0        │
# │ chr1       ┆ 1      ┆ 4    ┆ chr1             ┆ 5            ┆ 7          ┆ 2        │
# │ chr1       ┆ 4      ┆ 5    ┆ chr1             ┆ 0            ┆ 2          ┆ 2        │
# │ chr1       ┆ 4      ┆ 5    ┆ chr1             ┆ 5            ┆ 7          ┆ 1        │
# │ chr1       ┆ 7      ┆ 8    ┆ chr1             ┆ 5            ┆ 7          ┆ 0        │
# │ chr1       ┆ 7      ┆ 8    ┆ chr1             ┆ 6            ┆ 10         ┆ 0        │
# │ chr1       ┆ 10     ┆ 11   ┆ chr1             ┆ 6            ┆ 10         ┆ 0        │
# │ chr1       ┆ 10     ┆ 11   ┆ chr1             ┆ 8            ┆ 9          ┆ 1        │
# └────────────┴────────┴──────┴──────────────────┴──────────────┴────────────┴──────────┘
```


## Overlap

```python
import polars as pl
import poranges as po

df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts": [0, 8, 6, 5],
        "ends": [6, 9, 10, 7],
    }
)
# shape: (4, 3)
# ┌────────────┬────────┬──────┐
# │ chromosome ┆ starts ┆ ends │
# │ ---        ┆ ---    ┆ ---  │
# │ str        ┆ i64    ┆ i64  │
# ╞════════════╪════════╪══════╡
# │ chr1       ┆ 0      ┆ 6    │
# │ chr1       ┆ 8      ┆ 9    │
# │ chr1       ┆ 6      ┆ 10   │
# │ chr1       ┆ 5      ┆ 7    │
# └────────────┴────────┴──────┘

df2 = pl.DataFrame(
    {
        "starts": [6, 3, 1],
        "ends": [7, 8, 2],
        "genes": ["a", "b", "c"],
    }
)
# shape: (3, 3)
# ┌────────┬──────┬───────┐
# │ starts ┆ ends ┆ genes │
# │ ---    ┆ ---  ┆ ---   │
# │ i64    ┆ i64  ┆ str   │
# ╞════════╪══════╪═══════╡
# │ 6      ┆ 7    ┆ a     │
# │ 3      ┆ 8    ┆ b     │
# │ 1      ┆ 2    ┆ c     │
# └────────┴──────┴───────┘

df.interval.overlap(df2.lazy(), on=("starts", "ends"))
# shape: (3, 3)
# ┌────────────┬────────┬──────┐
# │ chromosome ┆ starts ┆ ends │
# │ ---        ┆ ---    ┆ ---  │
# │ str        ┆ i64    ┆ i64  │
# ╞════════════╪════════╪══════╡
# │ chr1       ┆ 0      ┆ 6    │
# │ chr1       ┆ 5      ┆ 7    │
# │ chr1       ┆ 6      ┆ 10   │
# └────────────┴────────┴──────┘
```

# Merge

```python
import polars as pl
import poranges as po

df = pl.DataFrame(
    {
        "starts": [4, 10, 7, 1],
        "ends": [5, 11, 8, 6],
        "weights": [-42, 0, 2, 1],
    }
)
# shape: (4, 3)
# ┌────────┬──────┬─────────┐
# │ starts ┆ ends ┆ weights │
# │ ---    ┆ ---  ┆ ---     │
# │ i64    ┆ i64  ┆ i64     │
# ╞════════╪══════╪═════════╡
# │ 4      ┆ 5    ┆ -42     │
# │ 10     ┆ 11   ┆ 0       │
# │ 7      ┆ 8    ┆ 2       │
# │ 1      ┆ 6    ┆ 1       │
# └────────┴──────┴─────────┘

res = df.interval.merge(df.lazy(), "starts", "ends")
# shape: (3, 5)
# ┌────────┬──────┬─────────────────────┬───────────────────┬───────────┐
# │ starts ┆ ends ┆ starts_before_merge ┆ ends_before_merge ┆ weights   │
# │ ---    ┆ ---  ┆ ---                 ┆ ---               ┆ ---       │
# │ i64    ┆ i64  ┆ list[i64]           ┆ list[i64]         ┆ list[i64] │
# ╞════════╪══════╪═════════════════════╪═══════════════════╪═══════════╡
# │ 1      ┆ 6    ┆ [1, 4]              ┆ [6, 5]            ┆ [1, -42]  │
# │ 7      ┆ 8    ┆ [7]                 ┆ [8]               ┆ [2]       │
# │ 10     ┆ 11   ┆ [10]                ┆ [11]              ┆ [0]       │
# └────────┴──────┴─────────────────────┴───────────────────┴───────────┘
```


# Testing

pyoframe is backed by unittests and property-based tests.

```bash
$ pytest tests/unit
$ pytest tests/property
```