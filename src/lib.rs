use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame
};
use pyo3_polars::error::PyPolarsErr;
use polars::prelude::*;

mod overlap_idxes;

#[pyfunction]
fn overlaps(
    df1: PyDataFrame,
    df2: PyDataFrame,
) -> PyResult<PyDataFrame> {
    let _df1: DataFrame = df1.into();
    let _df2: DataFrame = df2.into();
    Ok(
        PyDataFrame(
            overlap_idxes::overlap_idxes(
                &_df1,
                &_df2,
                "starts1",
                "ends1",
                "starts2",
                "ends2",
                false
            ).map_err(PyPolarsErr::from)?
        )
    )
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyoframe(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(overlaps, m)?)?;
    Ok(())
}