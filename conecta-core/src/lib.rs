use std::sync::Arc;

use postgres::fallible_iterator::FallibleIterator;
use postgres::types::WasNull;
use postgres::{NoTls, RowIter};
use r2d2_postgres::r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

use rayon::current_thread_index;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

pub mod destination;
pub mod metadata;
pub mod partition;
pub mod perf_logger;
pub mod schema;
pub mod source;

use crate::destination::get_arrow_builders;
use crate::metadata::create_partition_plan;
use crate::partition::PartitionConfig;
use crate::perf_logger::{log_memory_with_message, PerfLogger};
use crate::schema::NativeType;
use crate::source::get_source;

use arrow::array::{
    ArrayBuilder, ArrayRef, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    StringBuilder,
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use chrono::NaiveDate;

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
        .expect("Failed to create RecordBatch sexy")
}

macro_rules! append_column_value {
    (
        $unwrap:ident, $col_id:ident, $builder:ident, $native_type:ident,
        {
            $($type:pat => $builder_ty:ty, $value_ty:ty, $transform:expr),+ $(,)?
        }
    ) => {
        match $native_type {
            $(
                $type => {
                    let downcasted_builder = $builder
                        .as_any_mut()
                        .downcast_mut::<$builder_ty>()
                        .unwrap();
                    let unwrapped_value = $unwrap.try_get::<usize, $value_ty>($col_id);
                    match unwrapped_value {
                        Ok(v) => downcasted_builder.append_value(($transform)(v)),
                        Err(e) => {
                            // If the error was WasNull, we append a null.
                            if let Some(inner) = e.into_source() {
                                if inner.downcast_ref::<WasNull>().is_some() {
                                        downcasted_builder.append_null()
                                } else {
                                    panic!("Error trying to deserialize a type, {:?}", inner)
                                }
                            }
                        },
                    }
                }
            )+
            _ => panic!("Unsupported type: {:?}", $native_type),
        }
    };
}
pub fn read_sql(
    // Source.
    connection_string: &str,

    // Partition Configuration.
    queries: Vec<String>,
    partition_on: Option<String>,
    partition_range: Option<(i64, i64)>,
    partition_num: Option<u16>,
) -> (Vec<Vec<ArrayRef>>, crate::schema::Schema) {
    let mut perf_logger = PerfLogger::new_started();

    let partition_config = PartitionConfig::new(
        queries.clone(),
        partition_on,
        partition_num,
        partition_range,
    );

    perf_logger.log_checkpoint("Validating user parameters", false);

    let source = get_source(connection_string, None);
    let partition_plan = create_partition_plan(&source, partition_config);
    log::debug!("{:?}", partition_plan);
    perf_logger.log_checkpoint("Created query plan", true);

    let manager = PostgresConnectionManager::new(connection_string.parse().unwrap(), NoTls);
    let pool = Pool::builder()
        .max_size(partition_plan.query_data.len() as u32)
        .build(manager)
        .expect("Could not create a connection");

    let schema = source.get_schema_of(queries.clone().get(0).unwrap());

    let arrays: Vec<Vec<ArrayRef>> = partition_plan
        .query_data
        .into_par_iter()
        .map(|query| {
            let mut client = pool.get().unwrap();

            let query_number =
                client.query(format!("SELECT count(*) FROM ({:})", query).as_str(), &[]);
            let count: i64 = query_number.unwrap().get(0).unwrap().get(0);

            let rows = client
                .query_raw::<_, bool, _>(query.as_str(), vec![])
                .expect("Query failed");

            let mut builders: Vec<Box<dyn ArrayBuilder>> =
                get_arrow_builders(&schema, count as usize);

            log::debug!(
                "thread-{}: allocated {:?}x{:?}",
                current_thread_index().unwrap(),
                builders.len(),
                count
            );

            let column_types: Vec<NativeType> = schema
                .columns
                .iter()
                .map(|col| col.data_type.clone())
                .collect();

            for row in rows.iterator() {
                let unwrap = row.unwrap();
                for (col_id, builder) in builders.iter_mut().enumerate() {
                    let ty = column_types.get(col_id).unwrap();
                    append_column_value!(unwrap, col_id, builder, ty, {
                        NativeType::I32 => Int32Builder, i32, | v| v,
                        NativeType::F32 => Float32Builder, f32, | v | v,
                        NativeType::F64 => Float64Builder, f64, | v | v,
                        NativeType::Date32 => Date32Builder, NaiveDate, |v: NaiveDate|{
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            (v - epoch).num_days() as i32
                        },
                        NativeType::String => StringBuilder, String, | v | v
                    });
                }
            }

            let arrays: Vec<ArrayRef> = builders
                .into_iter()
                .map(|mut builder| builder.finish())
                .collect::<Vec<ArrayRef>>();
            return arrays;
        })
        .collect::<Vec<_>>();

    perf_logger.log_checkpoint("Finished loading data", true);

    perf_logger.log_elapsed();
    perf_logger.log_peak_memory();

    (arrays, schema)
}

// pub fn create_batches() -> (Vec<String>, Vec<Vec<(uintptr_t, uintptr_t)>>) {
//     // Define the schema: id: Int32, name: Utf8, score: Float64
//     use std::time::Instant;
//     let now = Instant::now();
//     env_logger::init();
//     let mut metadata = Metadata::new();
//     metadata.start();
//     log_memory();
//
//     let connection_string = "postgres://postgres:postgres@192.168.88.251:5400/postgres";
//     let manager = PostgresConnectionManager::new(connection_string.parse().unwrap(), NoTls);
//
//     // variables from users.
//     let queries: Vec<String> = vec!["\
//     SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax FROM lineitem10x".to_string()];
//
//     let partition_on = Some("l_orderkey".to_string());
//     let partition_range: Option<(i64, i64)> = None;
//     let partition_num: Option<u16> = Some(4);
//
//     metadata.print_step("Validating user parameters");
//     let partition_config =
//         PartitionConfig::new(queries, partition_on, partition_num, partition_range);
//
//     let source = get_source(connection_string, None);
//     let queryplan = create_partition_plan(&source, partition_config);
//     metadata.print_step("Fetching metadata");
//     let pool = Pool::builder()
//         .max_size(queryplan.query_data.len() as u32)
//         .build(manager)
//         .expect("t");
//
//     let rbs: Vec<_> = queryplan
//         .query_data
//         .into_par_iter()
//         .map(|query| {
//             let mut client: PooledConnection<PostgresConnectionManager<NoTls>> =
//                 pool.get().unwrap();
//             let rows: RowIter = client
//                 .query_raw::<_, bool, _>(query.as_str(), vec![])
//                 .expect("Query failed");
//
//             let mut builders: Vec<Box<dyn ArrayBuilder>> = vec![];
//             let num_of_cols = 8;
//             for i in 0..num_of_cols {
//                 if i == 5 {
//                     builders.push(Box::new(Float32Builder::with_capacity(1199969)))
//                 } else if i == 6 {
//                     builders.push(Box::new(Float32Builder::with_capacity(1199969)))
//                 } else {
//                     builders.push(Box::new(Int32Builder::with_capacity(1199969)))
//                 }
//             }
//
//             for row in rows.iterator() {
//                 match row {
//                     Ok(r) => {
//                         for i in 0..r.columns().len() {
//                             let v = r.try_get::<usize, i32>(i);
//                             match builders.get_mut(i) {
//                                 Some(builder) => {
//                                     // Try downcasting to Int32Builder
//                                     if let Some(downcasted_builder) =
//                                         builder.as_any_mut().downcast_mut::<Int32Builder>()
//                                     {
//                                         match v {
//                                             Ok(v2) => downcasted_builder.append_value(v2),
//                                             Err(_) => downcasted_builder.append_null(),
//                                         }
//                                     } else if let Some(downcasted_builder) =
//                                         builder.as_any_mut().downcast_mut::<Float32Builder>()
//                                     {
//                                         match v {
//                                             Ok(v2) => downcasted_builder.append_value(v2 as f32),
//                                             Err(_) => downcasted_builder
//                                                 .append_value(random_f32(&mut 234324)),
//                                         }
//                                     } else {
//                                         panic!("Unexpected builder type at column {}", i);
//                                     }
//                                 }
//                                 None => panic!("No builder found for column {}", i),
//                             }
//                         }
//                     }
//                     Err(e) => (),
//                 }
//             }
//
//             let arrays = builders
//                 .into_iter()
//                 .map(|mut builder| builder.finish())
//                 .collect::<Vec<ArrayRef>>();
//             make_record_batch(arrays)
//         })
//         .collect();
//     let mut rbb: Vec<RecordBatch> = vec![];
//     let mut schemas: Vec<SchemaRef> = vec![];
//
//     for (rb, schema) in rbs {
//         rbb.push(rb);
//         schemas.push(schema);
//     }
//     let t = to_ptrs(rbb);
//
//     log_memory_with_message(&format!(
//         "Created batches with to_ptrs: {:.2?}",
//         now.elapsed()
//     ));
//     log_peak_memory();
//     t
// }

// pub fn create_random_batches_ptr() -> (Vec<String>, Vec<Vec<(uintptr_t, uintptr_t)>>) {
//     // let rb = create_random_record_batch(10_000_000);
//     // create_batches()
// }
