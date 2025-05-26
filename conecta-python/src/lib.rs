use pyo3::prelude::*;
use conecta_core::test_from_core;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b + test_from_core() as usize).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn conecta(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
