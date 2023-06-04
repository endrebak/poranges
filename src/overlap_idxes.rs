use std::ops::Add;
use polars::prelude::*;

pub struct OverlapResult {
    pub idx1: Series,
    pub idx2: Series
}

pub fn overlap_idxes(
    df1: &DataFrame,
    df2: &DataFrame,
    starts1: &str,
    ends1: &str,
    starts2: &str,
    ends2: &str,
    closed: bool,
) -> Result<OverlapResult, PolarsError> {
    let df1 = df1.with_row_count("__idx__", None)?.sort([starts1, ends1], false)?;
    let df2 = df2.with_row_count("__idx__", None)?.sort([starts2, ends2], false)?;

    let sorted_side = if closed { SearchSortedSide::Right } else { SearchSortedSide::Left };

    let starts2in1 = find_insertion_idxes(
        df2.column(starts2)?, df1.column(starts1)?, SearchSortedSide::Left
    )?;

    let ends2in1 = find_insertion_idxes(
        df2.column(starts2)?, df1.column(ends1)?, sorted_side
    )?;

    let starts1in2 = find_insertion_idxes(
        df1.column(starts1)?, df2.column(starts2)?, SearchSortedSide::Right
    )?;

    let ends1in2 = find_insertion_idxes(
        df1.column(starts1)?, df2.column(ends2)?, sorted_side
    )?;

    let match2in1mask = ends2in1.u32()?.gt(starts2in1.u32()?);
    let match1in2mask = ends1in2.u32()?.gt(starts1in2.u32()?);
    println!("df1: {}", df1);
    println!("match2in1mask: {}", match2in1mask.into_series());
    println!("df2: {}", df2);
    println!("match1in2mask: {}", match1in2mask.into_series());

    Ok(
        OverlapResult {
            idx1: Series::new("", &[0]),
            idx2: Series::new("", &[0])
        }
    )
}

fn find_insertion_idxes(
    v1: &Series, v2: &Series, sorted_side: SearchSortedSide
) -> Result<Series, PolarsError> {
    Ok(
        search_sorted(
            v1,
            v2,
            sorted_side,
            false
        )?.into_series()
    )
}


fn arange_multi(
    starts: &Series, stops: &Series
) -> Result<Series, PolarsError> {
    let _lengths = (stops - starts).cast(&DataType::UInt32)?;
    let _cat_starts = starts.repeat_by(_lengths.u32()?)?.explode()?;
    let df_length = _cat_starts.len() as u32;
    let arange_len: Series = (0..df_length).into_iter().collect();

    let __s = _lengths.cumsum(false).subtract(&_lengths)?.repeat_by(_lengths.u32()?)?.explode()?;
    let _lengths_cumsum = arange_len.subtract(&__s)?;

    Ok(_lengths_cumsum.add(_cat_starts))
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

    #[test]
    fn test_simple() -> Result<(), PolarsError> {
        let f1 = &df!(
            "start" => &[3, 8, 5],
            "end" => &[6, 9, 7]
        )?;

        let f2 = &df!(
            "start2" => &[1, 6],
            "end2" => &[2, 7]
        )?;
        let res = overlap_idxes(f1, f2, "start", "end", "start2", "end2", false)?;
        println!("{:?}", res.idx1);
        panic!("");

        assert_eq!(
            res.idx1, Series::new("", &[2])
        );
        Ok(())
    }

    // #[test]
    // fn test_join() -> Result<(), PolarsError> {
    //     let start_col = "start";
    //     let end_col = "end";
    //     let start_col2 = "start2";
    //     let end_col2 = "end2";
    //     let df = &df!(
    //         "chromosome" => &["chr1", "chr1", "chr1", "chr1"],
    //         start_col => &[0, 8, 6, 5],
    //         end_col => &[6, 9, 10, 7],
    //         "genes" => &["a", "b", "c", "d"]
    //     )?;

    //     let df2 = &df!(
    //         start_col2 => &[6, 3, 1],
    //         end_col2 => &[7, 8, 2],
    //     )?;

    //     let res = join(
    //         df,
    //         df2,
    //         start_col,
    //         end_col,
    //         start_col2,
    //         end_col2,
    //         false
    //     )?.sort(["start", "end", "start2", "end2"], false)?;

    //     assert_eq!(
    //         df!(
    //             "chromosome" => &["chr1", "chr1", "chr1", "chr1", "chr1", "chr1"],
    //             start_col => &[0, 0, 5, 5, 6, 6],
    //             end_col => &[6, 6, 7, 7, 10, 10],
    //             "genes" => &["a", "a", "d", "d", "c", "c"],
    //             start_col2 => &[1, 3, 1, 6, 1, 6],
    //             end_col2 => &[2, 8, 2, 7, 2, 7],
    //         )?,
    //         res
    //     );
    //     Ok(())
    // }
}
