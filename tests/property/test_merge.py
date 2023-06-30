import poranges.register_interval_namespace

from hypothesis import given, settings, reproduce_failure

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB
)
@given(df=interval_df())
def test_merge(df):
    res_pyranges = to_pyranges(df).merge().df
    print(df)
    res_poranges = df.interval.merge(starts="Start", ends="End").collect()

    print(res_poranges)
    print(res_pyranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges, comparison_cols=("Start", "End"))
