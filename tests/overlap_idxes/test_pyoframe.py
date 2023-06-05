import polars as pl
import numpy as np

import bioframe.core.arrops as arrops

import pyoframe as pf

import polars as pl

import pytest

starts1 = [0, 8, 6, 5]
ends1 = [6, 9, 10, 7]

starts2 = [6, 3, 1]
ends2 = [7, 8, 2]

df = pl.DataFrame(
    {
        "starts1": starts1,
        "ends1": ends1,
    }
)

df2 = pl.DataFrame(
    {
        "starts2": starts2,
        "ends2": ends2,
    }
)

def test_overlap_indices():
    print(df)

    print(df2)
    idxes = pf.overlap_indices(df, df2)
    result = overlap_intervals(
        np.array(starts1),
        np.array(ends1),
        np.array(starts2),
        np.array(ends2)
    )
    print("--- pyoframe ---")
    print(idxes.idx1)
    print(idxes.idx2)
    print("--- bioframe ---")
    print(result)
    assert 0


def overlap_intervals(starts1, ends1, starts2, ends2, closed=False, sort=False):
    starts1 = np.asarray(starts1)
    ends1 = np.asarray(ends1)
    starts2 = np.asarray(starts2)
    ends2 = np.asarray(ends2)

    # Concatenate intervals lists
    n1 = len(starts1)
    n2 = len(starts2)
    ids1 = np.arange(0, n1)
    ids2 = np.arange(0, n2)

    # Sort all intervals together
    order1 = np.lexsort([ends1, starts1])
    order2 = np.lexsort([ends2, starts2])
    starts1, ends1, ids1 = starts1[order1], ends1[order1], ids1[order1]
    starts2, ends2, ids2 = starts2[order2], ends2[order2], ids2[order2]

    # Find interval overlaps
    match_2in1_starts = np.searchsorted(starts2, starts1, "left")
    match_2in1_ends = np.searchsorted(starts2, ends1, "right" if closed else "left")
    # "right" is intentional here to avoid duplication
    match_1in2_starts = np.searchsorted(starts1, starts2, "right")
    match_1in2_ends = np.searchsorted(starts1, ends2, "right" if closed else "left")

    # Ignore self-overlaps
    match_2in1_mask = match_2in1_ends > match_2in1_starts
    match_1in2_mask = match_1in2_ends > match_1in2_starts
    match_2in1_starts, match_2in1_ends = (
        match_2in1_starts[match_2in1_mask],
        match_2in1_ends[match_2in1_mask],
    )
    match_1in2_starts, match_1in2_ends = (
        match_1in2_starts[match_1in2_mask],
        match_1in2_ends[match_1in2_mask],
    )
    print(- match_2in1_starts + match_2in1_ends)
    print("first arange multi")
    print(arange_multi(match_2in1_starts, match_2in1_ends))
    print("second arange multi")
    print(arange_multi(match_1in2_starts, match_1in2_ends))


    return np.block(
        [
            [
                np.repeat(ids1[match_2in1_mask], match_2in1_ends - match_2in1_starts)[
                :, None
                ],
                ids2[arange_multi(match_2in1_starts, match_2in1_ends)][:, None],
            ],
            [
                ids1[arange_multi(match_1in2_starts, match_1in2_ends)][:, None],
                np.repeat(ids2[match_1in2_mask], match_1in2_ends - match_1in2_starts)[
                :, None
                ],
            ],
        ]
    )



def arange_multi(starts, stops=None, lengths=None):
    if (stops is None) == (lengths is None):
        raise ValueError("Either stops or lengths must be provided!")

    if lengths is None:
        lengths = stops - starts

    if np.isscalar(starts):
        starts = np.full(len(stops), starts)

    # Repeat start position index length times and concatenate
    cat_start = np.repeat(starts, lengths)

    # Create group counter that resets for each start/length
    cat_counter = np.arange(lengths.sum()) - np.repeat(
        lengths.cumsum() - lengths, lengths
    )

    # Add group counter to group specific starts
    cat_range = cat_start + cat_counter

    return cat_range