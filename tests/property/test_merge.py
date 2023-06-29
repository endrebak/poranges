import pyoframe as pf  # pylint: disable=unused-import

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
    res_pyoframe = df.interval.merge(starts="Start", ends="End").collect()

    print(res_pyoframe)
    print(res_pyranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_pyoframe, comparison_cols=("Start", "End"))
