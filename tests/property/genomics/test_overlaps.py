from hypothesis import settings, given, reproduce_failure, seed

from tests.property.generate_intervals import interval_df
from tests.property.helpers import to_pyranges, compare_frames
from tests.property.hypothesis_settings import MAX_EXAMPLES, PRINT_BLOB

import poranges.register_genomics_namespace


@settings(
    max_examples=MAX_EXAMPLES,
    print_blob=PRINT_BLOB,
    deadline=None
)
@given(df=interval_df(), df2=interval_df())
def test_join(df, df2):
    strandedness = "same"
    strandedness = "opposite"
    print(df)
    print(df2)
    res_pyranges = to_pyranges(df).overlap(to_pyranges(df2), strandedness=strandedness).df
    res_poranges = df.genomics.overlap(df2, on=("Chromosome", "Start", "End", "Strand"), strand_join=strandedness).collect().to_pandas()
    print("PORANGES", res_poranges)
    print("PYRANGES", res_pyranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges, comparison_cols=("Chromosome", "Start", "End", "Strand"))
