use std::sync::Arc;

use postgres::NoTls;
use r2d2_postgres::r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

pub mod destination;
pub mod metadata;
pub mod partition;
pub mod perf_logger;
pub mod schema;
pub mod source;

use crate::metadata::create_partition_plan;
use crate::partition::PartitionConfig;
use crate::perf_logger::{perf_checkpoint, perf_start};
use crate::source::{get_source, Source, SourceType};

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::source::postgres::PostgresSource;
use log::debug;

pub fn test_from_core() -> i32 {
    3
}

/// Given a vector `Vec<T>` where `T` is `Vec<ArrayRef>` representing a chunk of the same table
/// transform all `T` to a `arrow::datatype::RecordBatch`.
///
/// All record batches are assumed to have the same schema, they are not concatenated
/// to avoid memory copying.
pub fn make_record_batches(arrays: Vec<Vec<ArrayRef>>, col_names: Vec<String>) -> Vec<RecordBatch> {
    arrays
        .into_iter()
        .map(|chunk| make_record_batch(chunk, col_names.clone()))
        .collect::<Vec<RecordBatch>>()
}

pub fn make_record_batch(arrays: Vec<ArrayRef>, col_names: Vec<String>) -> RecordBatch {
    let fields: Vec<Field> = arrays
        .iter()
        .zip(col_names)
        .map(|(array, name)| Field::new(&name, array.data_type().clone(), true))
        .collect();

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(SchemaRef::from(schema.clone()), arrays)
        .expect("Failed to create RecordBatch")
}

pub fn read_sql(
    // Source.
    connection_string: &str,

    // Partition Configuration.
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,

    // Extra configuration.
    max_pool_size: Option<u32>,
    disable_preallocation: bool,
) -> (Vec<Vec<ArrayRef>>, crate::schema::Schema) {
    perf_start();

    let partition_config = PartitionConfig::new(
        queries.clone(),
        partition_on,
        partition_num,
        partition_range,
        disable_preallocation,
    );

    let max_pool_size = max_pool_size.unwrap_or_else(|| {
        // If the user does not provide max_pool_size, we will set it to the number of partitions
        // that we will end up using, we cannot use any info from the partition_plan yet.
        // We will use a dirty but correct calculation of how many threads/partitions we will use.
        match queries.len() {
            1 => partition_num.unwrap_or(1) as u32,
            _ => queries.len() as u32,
        }
    });

    perf_checkpoint("Validating user parameters", false);

    let source_type = get_source(connection_string, None);
    let source: Box<dyn Source>;

    match source_type {
        SourceType::Postgres => {
            let manager = PostgresConnectionManager::new(connection_string.parse().unwrap(), NoTls);
            let pool = Pool::builder()
                .max_size(max_pool_size)
                .build(manager)
                .expect("Could not create a pool of connections");
            source = Box::new(PostgresSource { pool })
        }
        _ => panic!("Source is not supported"),
    }

    let partition_plan = create_partition_plan(&source, partition_config);

    debug!("{:?}", partition_plan);

    perf_checkpoint("Created query plan", true);

    let schema: crate::schema::Schema = source.get_schema_of(queries.clone().get(0).unwrap());
    source.process_partition_plan(partition_plan, schema)
}
