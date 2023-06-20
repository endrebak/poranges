use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame
};
use pyo3_polars::error::PyPolarsErr;
use polars::prelude::*;
use std::collections::HashMap;


fn split_apply_combine(df: DataFrame) -> Result<HashMap<&'static str, DataFrame>, PolarsError> {
    // let mut cs_to_df = HashMap::new();
    let cs = df.column("chromosome")?;
    let ucs = cs.unique()?.utf8().into_iter().collect();
    // let v = [
    //     for c in .into_iter().collect::<Vec<String>>() {
    //         (c, df.filter(pl.col("chromosome")?.eq(&c)?)?);
    //     }
    // ];
    Ok(
        HashMap::from([("chr1", df)])
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sac() -> Result<(), PolarsError> {
        let start_col = "start";
        let end_col = "end";
        let start_col2 = "start2";
        let end_col2 = "end2";
        let df = &df!(
            "chromosome" => &["chr1", "chr2", "chr1", "chr3"],
            start_col => &[0, 8, 6, 5],
            end_col => &[6, 9, 10, 7],
            "genes" => &["a", "b", "c", "d"]
        )?;

        let df2 = &df!(
            start_col2 => &[6, 3, 1],
            end_col2 => &[7, 8, 2],
        )?;

        assert_eq!(
            0, 1
        );
        Ok(())
    }
}
