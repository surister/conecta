use conecta_core::partition::PartitionConfig;
use conecta_core::source::get_source;
use conecta_core::{make_record_batches, test_from_core};
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_arrow::{PyTable};

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

#[pyfunction]
pub fn read_sql<'py>(
    py: Python,
    // Source.
    connection_string: &str,

    // Partition Configuration.
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,
) -> PyResult<PyObject> {
    env_logger::init();

    let (arrays, schema) = py.allow_threads(|| {
        conecta_core::read_sql(
            connection_string,
            queries,
            partition_on,
            partition_range,
            partition_num,
        )
    });

    let record_batches = make_record_batches(
        arrays,
        schema
            .clone()
            .columns
            .into_iter()
            .map(|col| col.name)
            .collect(),
    );

    Ok(PyTable::try_new(record_batches, Arc::new(schema.to_arrow()))?.to_pyarrow(py)?)
}

/// A Python module implemented in Rust.
#[pymodule]
fn conecta(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(create_partition_plan, m)?)?;
    m.add_function(wrap_pyfunction!(read_sql, m)?)?;
    Ok(())
}
