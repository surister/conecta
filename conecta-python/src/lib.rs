use conecta_core::partition::PartitionConfig;
use conecta_core::source::get_source;
use conecta_core::test_from_core;
use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b + test_from_core() as usize).to_string())
}
#[pyfunction]
fn two(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b + test_from_core() as usize).to_string())
}
#[pyfunction]
fn create_partition_plan(
    // Source.
    connection_string: &str,

    // Partition Configuration.
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,
) -> PyResult<String> {
    let partition_config =
        PartitionConfig::new(queries, partition_on, partition_num, partition_range);

    let source = get_source(connection_string, None);
    let plan = conecta_core::metadata::create_partition_plan(&source, partition_config);
    let json = serde_json::to_string(&plan).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
    })?;
    Ok(json)
}

/// A Python module implemented in Rust.
#[pymodule]
fn conecta(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(create_partition_plan, m)?)?;
    Ok(())
}
