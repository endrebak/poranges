import poranges.register_interval_namespace

from hypothesis import given, settings, reproduce_failure

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB,
    deadline=None
)
@given(df=interval_df(), df2=interval_df())
def test_join(df, df2):
    print(df)
    print(df2)
    res_pyranges = to_pyranges(df).join(to_pyranges(df2), suffix="_right", apply_strand_suffix=False).df
    print("PYRANGES", res_pyranges)
    res_poranges = df.interval.join(df2, on=("Start", "End"), by=["Chromosome"]).collect().to_pandas()
    print("PORANGES", res_poranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges)
