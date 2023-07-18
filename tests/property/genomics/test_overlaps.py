from hypothesis import settings, given, reproduce_failure

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
@reproduce_failure('6.46.9', b'AXicY2BAAAAADAAB')
def test_join(df, df2):
    print(df)
    print(df2)
    res_pyranges = to_pyranges(df).overlap(to_pyranges(df2), strandedness="opposite").df
    print("PYRANGES", res_pyranges)
    res_poranges = df.genomics.overlap(df2, on=("Chromosome", "Start", "End", "Strand"), strand_join="opposite").collect().to_pandas()
    print("PORANGES", res_poranges)
    compare_frames(pd_df=res_pyranges, pl_df=res_poranges)
