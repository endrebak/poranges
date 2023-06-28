import pyoframe as pf  # pylint: disable=unused-import

from hypothesis import given, settings

from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB
from tests.property.generate_intervals import interval_df


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB
)
@given(df=interval_df(), df2=interval_df())
def test_closest(df, df2):
    res_pyranges = to_pyranges(df).nearest(to_pyranges(df2), suffix="_right", apply_strand_suffix=False).df
    res_pyoframe = df.interval.closest(df2, on=("Start", "End")).collect()

    print(res_pyoframe)
    print(res_pyranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_pyoframe)
