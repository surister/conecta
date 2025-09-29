use log::debug;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyTable;

use conecta_core::{_create_partition_plan, make_record_batches};

#[pyfunction]
fn create_partition_plan(
    // Source.
    connection_string: &str,

    // Partition Configuration.
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,

    // Extra configuration.
    max_pool_size: Option<u32>,
    preallocation: bool,
) -> PyResult<String> {
    let plan = _create_partition_plan(
        connection_string,
        queries,
        partition_on,
        partition_range,
        partition_num,
        max_pool_size,
        preallocation,
    );

    let json = serde_json::to_string(&plan).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
    })?;

    Ok(json)
}

#[pyfunction]
pub fn read_sql(
    py: Python,
    // Source
    connection_string: &str,

    // Partition Configuration
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,

    // Extra configuration
    max_pool_size: Option<u32>,
    preallocation: bool,

    // Return configuration
    return_backend: String,
) -> PyArrowResult<PyObject> {
    let _ = env_logger::try_init();

    let (arrays, schema) = py.allow_threads(|| {
        conecta_core::read_sql(
            connection_string,
            queries,
            partition_on,
            partition_range,
            partition_num,

            // Extra configuration
            max_pool_size,
            preallocation,
        )
    });

    let rbs = make_record_batches(
        arrays,
        schema
            .clone()
            .columns
            .into_iter()
            .map(|col| col.name)
            .collect(),
    );

    debug!("num_rows, num_columns, buffer_size_bytes");
    for rb in &rbs {
        debug!(
            "{:?}, {:?}, {:?}",
            rb.num_rows(),
            rb.num_columns(),
            rb.get_array_memory_size()
        );
    }

    let table = PyTable::try_new(rbs, Arc::new(schema.to_arrow()));

    match return_backend.as_str() {
        "arro3" => Ok(table?.to_arro3(py)?.into()),
        "nanoarrow" => Ok(table?.to_nanoarrow(py)?.into()),
        // We default to pyarrow, its also default on conecta-python
        _ => Ok(table?.to_pyarrow(py)?.into()),
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn conecta(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_partition_plan, m)?)?;
    m.add_function(wrap_pyfunction!(read_sql, m)?)?;
    Ok(())
}
