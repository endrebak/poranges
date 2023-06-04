use polars::prelude::*;
use crate::overlap_idxes;

pub fn overlaps(
    df1: &DataFrame,
    df2: &DataFrame,
    starts_col1: &str,
    ends_col1: &str,
    starts_col2: &str,
    ends_col2: &str,
    closed: bool,
) -> Result<DataFrame, PolarsError> {
    let res = overlap_idxes::overlap_idxes(
        df1,
        df2,
        starts_col1,
        ends_col1,
        starts_col2,
        ends_col2,
        closed
    )?;
    Ok(
        res.df1.filter(&res.match2in1mask)?
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlaps_simple() -> Result<(), PolarsError> {
        let start_col = "start";
        let end_col = "end";
        let start_col2 = "start2";
        let end_col2 = "end2";
        let df = &df!(
            "chromosome" => &["chr1", "chr1", "chr1", "chr1"],
            start_col => &[0, 8, 6, 5],
            end_col => &[6, 9, 10, 7],
        )?;

        let df2 = &df!(
            start_col2 => &[6, 3, 1],
            end_col2 => &[7, 8, 2],
        )?;

        let res = overlaps(
            df,
            df2,
            start_col,
            end_col,
            start_col2,
            end_col2,
            false
        )?.sort(["start", "end"], false)?;

        assert_eq!(
            df!(
                "chromosome" => &["chr1", "chr1", "chr1"],
                start_col => &[0, 5, 6],
                end_col => &[6, 7, 10],
            )?,
            res
        );
        Ok(())
    }

    fn test_overlaps() -> Result<(), PolarsError> {
        let df1 = &df!(
            "start" => [10029, 10043, 10114, 10336, 110336, 10041, 10068, 10091, 10217, 10331],
            "end" => [10048, 10062, 10133, 10355, 110355, 10060, 10087, 10110, 10236, 10350]
        )?;

        let df2 = &df!(
           "start" => [10163, 10370, 16146, 16154, 16199, 10078, 10169, 10172, 10239, 20048],
           "end" => [10182, 10389, 16165, 16173, 16218, 10097, 10188, 10191, 10258, 20067]
       )?;

        assert_eq!(
            df!(
                "start" => [10068, 10091],
                "end" => [10087, 10110]
            )?,
            overlaps(
                df1,
                df2,
                "start",
                "end",
                "start",
                "end",
                false
            )?
        );
        Ok(())
    }
}

