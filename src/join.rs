use polars::prelude::*;
use crate::overlap_idxes;
use polars::functions::hor_concat_df;

use polars_lazy::prelude::*;

pub fn join(
    df1: &DataFrame,
    df2: &DataFrame,
    starts_col1: &str,
    ends_col1: &str,
    starts_col2: &str,
    ends_col2: &str,
    closed: bool,
) -> Result<DataFrame, PolarsError> {
    // FIXME: Support columns having the same name.
    //        This should probably be added to hor_concat_df.
    //        Make issue?

    let res = overlap_idxes::overlap_idxes(
        df1,
        df2,
        starts_col1,
        ends_col1,
        starts_col2,
        ends_col2,
        closed
    )?;

    let res1 = repeat_df_by_matches(
        &res.df1.filter(&res.match2in1mask)?,
        &res.starts2in1,
        &res.ends2in1,
    )?;
    println!("res1: {}", res1);

    let ar1 = arange_multi(&res.starts2in1, &res.ends2in1)?;
    println!("ar1: {}", ar1);
    let res2_matching_res_1 = res.df2.take(&ar1.u32()?.clone())?;

    let res2 = repeat_df_by_matches(
        &res.df2.filter(&res.match1in2mask)?,
        &res.starts1in2,
        &res.ends1in2,
    )?;
    println!("res2: {}", res2);

    let ar2 = arange_multi(&res.starts1in2, &res.ends1in2)?;
    println!("ar2: {}", ar2);
    let res1_matching_res_2 = res.df1.take(&ar2.u32()?.clone())?;

    Ok(
        concat(
            &[
                hor_concat_df(&[res1, res2_matching_res_1])?.lazy(),
                hor_concat_df(&[res1_matching_res_2, res2])?.lazy(),
            ],
            false,
            false
        )?.drop_nulls(None).collect()?
    )
}
//

fn repeat_df_by_matches(
    df: &DataFrame,
    matching_starts: &Series,
    matching_ends: &Series,
) -> Result<DataFrame, PolarsError> {
    let nb_repeats = matching_ends.subtract(matching_starts)?.into_series();
    Ok(
        df.clone().lazy().select(
            [
                all().repeat_by(
                    lit(nb_repeats)
                ).explode().drop_nulls()
            ]
        ).collect()?
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_arange_multi_simple() -> Result<(), PolarsError> {
        assert_eq!(
            Series::new("", &[0i64, 1, 2, 5, 6, 7, 10, 11, 12]),
            arange_multi(
                &Series::new("starts", &[0, 5, 10]),
                &Series::new("ends", &[3, 8, 13]),
            )?
        );
        Ok(())
    }

    // #[test]
    fn test_simple_join() -> Result<(), PolarsError> {
        let f1 = &df!(
            "chromosome" => &["chr1", "chr1", "chr1"],
            "start" => &[3, 8, 5],
            "end" => &[6, 9, 7]
        )?;

        let f2 = &df!(
            "start2" => &[1, 6],
            "end2" => &[2, 7]
        )?;

        let res = join(
            f1,
            f2,
            "start",
            "end",
            "start2",
            "end2",
            false
        )?.sort(["start", "end"], false)?;
        assert_eq!(
            df!(
                "chromosome" => &["chr1"],
                "start" => &[5],
                "end" => &[7],
                "start2" => &[6],
                "end2" => &[7]
            )?,
            res
        );
        Ok(())
    }

    #[test]
    fn test_join() -> Result<(), PolarsError> {
        let start_col = "start";
        let end_col = "end";
        let start_col2 = "start2";
        let end_col2 = "end2";
        let df = &df!(
            "chromosome" => &["chr1", "chr1", "chr1", "chr1"],
            start_col => &[0, 8, 6, 5],
            end_col => &[6, 9, 10, 7],
            "genes" => &["a", "b", "c", "d"]
        )?;

        let df2 = &df!(
            start_col2 => &[6, 3, 1],
            end_col2 => &[7, 8, 2],
        )?;

        let res = join(
            df,
            df2,
            start_col,
            end_col,
            start_col2,
            end_col2,
            false
        )?.sort(["start", "end", "start2", "end2"], false)?;

        assert_eq!(
            df!(
                "chromosome" => &["chr1", "chr1", "chr1", "chr1", "chr1", "chr1"],
                start_col => &[0, 0, 5, 5, 6, 6],
                end_col => &[6, 6, 7, 7, 10, 10],
                "genes" => &["a", "a", "d", "d", "c", "c"],
                start_col2 => &[1, 3, 1, 6, 1, 6],
                end_col2 => &[2, 8, 2, 7, 2, 7],
            )?,
            res
        );
        Ok(())
    }
}
