use std::ops::Add;
use polars::prelude::*;
use polars_lazy::prelude::*;
use polars::functions::hor_concat_df;

pub fn overlap_idxes(
    df1: &DataFrame,
    df2: &DataFrame,
    starts1: &str,
    ends1: &str,
    starts2: &str,
    ends2: &str,
    closed: bool,
) -> Result<DataFrame, PolarsError> {
    let df1_sorted = df1.sort([starts1, ends1], false)?;
    let df2_sorted = df2.sort([starts2, ends2], false)?;
    // println!("df1: {}", df1_sorted);
    // println!("df2: {}", df2_sorted);

    let sorted_side = if closed { SearchSortedSide::Right } else { SearchSortedSide::Left };

    let starts2in1 = find_insertion_idxes(
        df2_sorted.column(starts2)?, df1_sorted.column(starts1)?, SearchSortedSide::Left
    )?;

    let ends2in1 = find_insertion_idxes(
        df2_sorted.column(starts2)?, df1_sorted.column(ends1)?, sorted_side
    )?;

    // println!("starts {}", df2_sorted.column(starts2)?);
    let starts1in2 = find_insertion_idxes(
        df1_sorted.column(starts1)?, df2_sorted.column(starts2)?, SearchSortedSide::Right
    )?;
    // println!("starts2in1: {}", starts2in1);

    let ends1in2 = find_insertion_idxes(
        df1_sorted.column(starts1)?, df2_sorted.column(ends2)?, sorted_side
    )?;
    // println!("ends2in1: {}", ends2in1);

    let match2in1mask = ends2in1.u32()?.gt(starts2in1.u32()?);
    let match1in2mask = ends1in2.u32()?.gt(starts1in2.u32()?);
    // println!("match1in2mask: {}", match1in2mask.clone().into_series());
    // println!("match2in1mask: {}", match2in1mask.clone().into_series());

    let match_1in2_starts = starts1in2.filter(&match1in2mask)?;
    let match_1in2_ends = ends1in2.filter(&match1in2mask)?;

    let match_2in1_starts = starts2in1.filter(&match2in1mask)?;
    let match_2in1_ends = ends2in1.filter(&match2in1mask)?;
    // println!("match_2in1_starts: {}", match_2in1_starts);
    // println!("match_2in1_ends: {}", match_2in1_ends);

    let reps1 = repeat_df_by_matches(
        &df1_sorted.filter(&match2in1mask)?,
        &match_2in1_starts,
        &match_2in1_ends,
    )?;
    // println!("reps: {}", reps1);
    let reps2 = repeat_df_by_matches(
        &df2_sorted.filter(&match1in2mask)?,
        &match_1in2_starts,
        &match_1in2_ends,
    )?;
    // println!("reps2: {}", reps2);

    // Below we want to get
    let indices = arange_multi(&match_1in2_starts, &match_1in2_ends)?.drop_nulls();
    // println!("indices: {}", indices);
    let taken_indices = df1_sorted.take(
        indices.u32()?
    )?;
    // println!("taken_indices: {}", taken_indices);

    let indices2 = arange_multi(&match_2in1_starts, &match_2in1_ends)?.drop_nulls();
    // println!("indices2: {}", indices2);
    let taken_indices2 = df2_sorted.take(
        indices2.u32()?
    )?;
    // println!("taken_indices2: {}", taken_indices2);

    Ok(
        concat(
            &[
                hor_concat_df(&[reps1, taken_indices2])?.lazy(),
                hor_concat_df(&[taken_indices, reps2])?.lazy(),
            ],
            false,
            false
        )?.drop_nulls(None).collect()?
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
        // TODO: Write assertion
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

        let res = overlap_idxes(df, df2, "start", "end", "start2", "end2", false)?;

        // TODO: Write assertion

        Ok(())
    }

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
