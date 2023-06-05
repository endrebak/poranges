use pyo3::prelude::*;
use pyo3_polars::{
    PyDataFrame,
    PySeries,
};
use pyo3_polars::error::PyPolarsErr;
use polars::prelude::*;

mod overlap_idxes;

#[pyclass]
pub struct OverlapIndices {
    #[pyo3(get)]
    pub idx1: PySeries,
    #[pyo3(get)]
    pub idx2: PySeries
}

#[pyfunction]
fn overlap_indices(
    df1: PyDataFrame,
    df2: PyDataFrame,
) -> PyResult<OverlapIndices> {
    let _df1: DataFrame = df1.into();
    let _df2: DataFrame = df2.into();
    let idxes = overlap_idxes::overlap_idxes(
        &_df1,
        &_df2,
        "starts1",
        "ends1",
        "starts2",
        "ends2",
        false
    ).map_err(PyPolarsErr::from)?;
    Ok(
        OverlapIndices {
            idx1: PySeries(idxes.idx1),
            idx2: PySeries(idxes.idx2)
        }
    )
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyoframe(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(overlap_indices, m)?)?;
    m.add_class::<OverlapIndices>()?;
    Ok(())
}