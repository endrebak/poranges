# pyoframe

GenomicRanges in Rust, accessible from Python.

In R&D phase. Not even in alpha.

# Example

```
import polars as pl

import pyoframe as pf

df = pl.DataFrame(
    {
        "chromosome": ["chr1", "chr1", "chr1", "chr1"],
        "starts1": [0, 8, 6, 5],
        "ends1": [6, 9, 10, 7],
        "genes": ["a", "b", "c", "d"]
    }
)
df2 = pl.DataFrame(
    {
        "starts2": [6, 3, 1],
        "ends2": [7, 8, 2],
    }
)

j = pf.overlaps(df, df2)
print(j)

# shape: (6, 6)
# ┌────────────┬─────────┬───────┬───────┬─────────┬───────┐
# │ chromosome ┆ starts1 ┆ ends1 ┆ genes ┆ starts2 ┆ ends2 │
# │ ---        ┆ ---     ┆ ---   ┆ ---   ┆ ---     ┆ ---   │
# │ str        ┆ i64     ┆ i64   ┆ str   ┆ i64     ┆ i64   │
# ╞════════════╪═════════╪═══════╪═══════╪═════════╪═══════╡
# │ chr1       ┆ 0       ┆ 6     ┆ a     ┆ 1       ┆ 2     │
# │ chr1       ┆ 0       ┆ 6     ┆ a     ┆ 3       ┆ 8     │
# │ chr1       ┆ 5       ┆ 7     ┆ d     ┆ 6       ┆ 7     │
# │ chr1       ┆ 6       ┆ 10    ┆ c     ┆ 6       ┆ 7     │
# │ chr1       ┆ 5       ┆ 7     ┆ d     ┆ 3       ┆ 8     │
# │ chr1       ┆ 6       ┆ 10    ┆ c     ┆ 3       ┆ 8     │
# └────────────┴─────────┴───────┴───────┴─────────┴───────┘
```

## Developing

### Setup

```bash
# Build Rust code and create Python bindings
maturin develop

# Run tests
pytest
```

