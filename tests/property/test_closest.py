import itertools

import pytest

import numpy as np

import poranges.register_interval_namespace

import bioframe as bf

from hypothesis import given, settings, reproduce_failure, seed

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df, non_empty_interval_df


# Ensures that the directionality is correct (not easy to test for in bioframe)
@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB,
    deadline=None
)
@given(df=interval_df(), df2=interval_df())
@pytest.mark.parametrize(
    "include_overlapping,distance_col,direction",
    itertools.product([False, True], ["Distance"], ["upstream", "downstream"])
)
def test_closest(df, df2, include_overlapping, distance_col, direction):
    print("TEST_CLOSEST")
    print(df.to_init_repr())
    print(df2.to_init_repr())
    gr = to_pyranges(df)
    gr.Strand = "+"
    gr2 = to_pyranges(df2)
    gr2.Strand = "+"
    print(gr)
    print(gr2)
    res_pyranges = gr.nearest(
        gr2,
        suffix="_right",
        overlap=include_overlapping,
        apply_strand_suffix=False,
        how=direction
    ).df
    print("py done")
    print(res_pyranges)
    res_poranges = df.interval.closest(
        df2,
        on=("Start", "End"),
        by=["Chromosome"],
        include_overlapping=include_overlapping,
        distance_col="Distance",
        direction="left" if direction == "upstream" else "right",
    ).collect().drop_nulls().to_pandas()

    print("PYRANGES")
    print(res_pyranges)
    print("PORANGES")
    print(res_poranges)
    # We do not check the start right and end right columns because they are not always the same
    compare_frames(
        pd_df=res_pyranges,
        pl_df=res_poranges,
        comparison_cols=("Chromosome", "Start", "End", "Distance")
    )


# Test k closest in bioframe (it seems pyranges contains small bugs)
@settings(
    max_examples=50,
    print_blob=PRINT_BLOB,
    deadline=None
)
@given(df=non_empty_interval_df(), df2=non_empty_interval_df())
@pytest.mark.parametrize(
    "k,include_overlapping,distance_col",
    itertools.product([2, 3, 7], [True], [None, "Distance"])
)
def test_k_closest(df, df2, k, include_overlapping, distance_col):
    print("RUNNING_BIOFRAME")
    res_bioframe = bf.closest(
        df.to_pandas().astype({"Start": np.int64, "End": np.int64}),
        df2.to_pandas().astype({"Start": np.int64, "End": np.int64}),
        return_distance=bool(distance_col),
        ignore_overlaps=not include_overlapping,
        suffixes=("", "_right"),
        cols1=("Chromosome", "Start", "End"),
        cols2=("Chromosome", "Start", "End"),
        k=k
    ).rename(columns={"distance": "Distance"})
    print("TEST_CLOSEST")
    print(df.to_init_repr())
    print(df2.to_init_repr())
    res_poranges = df.interval.closest(
        df2,
        on=("Start", "End"),
        by=["Chromosome"],
        distance_col=distance_col,
        include_overlapping=include_overlapping,
        k=k
    ).collect().to_pandas()

    print("BIOFRAME")
    print(res_bioframe)
    print("PORANGES")
    print(res_poranges)
    # We do not check the start right and end right columns because they are not always the same
    compare_frames(pd_df=res_bioframe, pl_df=res_poranges, comparison_cols=("Chromosome", "Start", "End"))
