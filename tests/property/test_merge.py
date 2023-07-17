import poranges.register_interval_namespace

from hypothesis import given, settings, reproduce_failure

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB,
    deadline=None,
)
@given(df=interval_df())
def test_merge(df):
    res_pyranges = to_pyranges(df).merge().df
    res_poranges = df.interval.merge(starts="Start", ends="End", by=["Chromosome"], keep_original_columns=False).collect().to_pandas()

    print("PORANGES:", res_poranges)
    print("PYRANGES:", res_pyranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges, comparison_cols=("Start", "End"))
