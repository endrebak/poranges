use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame,
    PySeries,
};
use pyo3_polars::error::PyPolarsErr;
use polars::prelude::*;

mod overlap_idxes;
// mod join;
// mod overlaps;
//
// /// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }
//
// #[pyfunction]
// fn add_one_to_row(s: PySeries) -> PyResult<PySeries> {
//     Ok(PySeries(s.as_ref() + 1))
// }
//
// #[pyfunction]
// fn range_join(
//     df1: PyDataFrame,
//     df2: PyDataFrame,
// ) -> PyResult<PyDataFrame> {
//     let _df1: DataFrame = df1.into();
//     let _df2: DataFrame = df2.into();
//     Ok(
//         PyDataFrame(
//             join::join(
//                 &_df1,
//                 &_df2,
//                 "starts1",
//                 "ends1",
//                 "starts2",
//                 "ends2",
//                 false
//             ).map_err(PyPolarsErr::from)?
//         )
//     )
// }
//
// /// A Python module implemented in Rust.
// #[pymodule]
// fn pyoframe(_py: Python, m: &PyModule) -> PyResult<()> {
//     m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
//     m.add_function(wrap_pyfunction!(add_one_to_row, m)?)?;
//     m.add_function(wrap_pyfunction!(range_join, m)?)?;
//     Ok(())
// }