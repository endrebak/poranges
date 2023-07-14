import poranges.register_interval_namespace

from hypothesis import given, settings, reproduce_failure, seed

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB
)
@given(df=interval_df(), df2=interval_df())
@reproduce_failure('6.46.9', b'AXicY2PABZixCbJiCjFiCjExAAAD+QAS')
def test_closest(df, df2):
    print("TEST_CLOSEST")
    print(df.to_init_repr())
    print(df2.to_init_repr())
    res_pyranges = to_pyranges(df).nearest(to_pyranges(df2), suffix="_right", apply_strand_suffix=False).df
    # PyRanges counts bookended intervals as having a distance of 0, while poranges counts them as having a distance of 1
    # if "Distance" in res_pyranges.columns:
    #     res_pyranges.loc[res_pyranges.Distance > 1, "Distance"] = res_pyranges.loc[res_pyranges.Distance > 0, "Distance"] + 1
    print("py done")
    print(res_pyranges)
    res_poranges = df.interval.closest(df2, on=("Start", "End"), by=["Chromosome"], distance_col="Distance").collect().to_pandas()

    print("PYRANGES")
    print(res_pyranges)
    print("PORANGES")
    print(res_poranges)
    # We do not check the start right and end right columns because they are not always the same
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges, comparison_cols=["Chromosome", "Start", "End", "Distance"])
