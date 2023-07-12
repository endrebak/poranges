import poranges.register_interval_namespace

from hypothesis import given, settings, reproduce_failure

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB
)
@given(df=interval_df(), df2=interval_df())
@reproduce_failure('6.46.9', b'AXicY2MgCTAisQEBqQAI')
def test_closest(df, df2):
    print("TEST_CLOSEST")
    print(df)
    print(df2)
    res_pyranges = to_pyranges(df).nearest(to_pyranges(df2), suffix="_right", apply_strand_suffix=False).df
    print("py done")
    res_poranges = df.interval.closest(df2, on=("Start", "End"), by=["Chromosome"]).collect().to_pandas()

    print("PYRANGES")
    print(res_pyranges)
    print("PORANGES")
    print(res_poranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges)
