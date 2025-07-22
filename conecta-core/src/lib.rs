use std::sync::Arc;

use postgres::types::WasNull;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{NoTls, RowIter};
use r2d2_postgres::r2d2::{Pool};
use r2d2_postgres::PostgresConnectionManager;

use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelIterator;

pub mod debug;
pub mod destination;
pub mod logger;
pub mod metadata;
pub mod partition;
pub mod schema;
pub mod source;

use crate::debug::Metadata;
use crate::destination::get_arrow_builders;
use crate::logger::{log_memory_with_message};
use crate::metadata::create_partition_plan;
use crate::partition::PartitionConfig;
use crate::schema::NativeType;
use crate::source::get_source;

use arrow::array::{
    ArrayBuilder,
    ArrayRef,
    Date32Builder,
    Float32Builder,
    Float64Builder,
    Int32Builder,
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
pub fn make_record_batches(
    arrays: Vec<Vec<ArrayRef>>,
    col_names: Vec<String>
) -> Vec<RecordBatch> {
    arrays
        .into_iter()
        .map(|chunk| make_record_batch(chunk, col_names.clone()))
        .collect::<Vec<RecordBatch>>()
}


pub fn make_record_batch(
    arrays: Vec<ArrayRef>,
    col_names: Vec<String>
) -> RecordBatch {
    let fields: Vec<Field> = arrays
        .iter()
        .zip(col_names)
        .map(|(array, name)| {
            Field::new(&name, array.data_type().clone(), true)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(
        SchemaRef::from(schema.clone()), arrays)
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
    let mut metadata = Metadata::new_started();
    let partition_config = PartitionConfig::new(
        queries.clone(),
        partition_on,
        partition_num,
        partition_range,
    );
    metadata.print_step("Validating user parameters");

    let source = get_source(connection_string, None);
    let queryplan = create_partition_plan(&source, partition_config);
     metadata.print_step("Created query plan");
    let manager = PostgresConnectionManager::new(connection_string.parse().unwrap(), NoTls);
    let pool = Pool::builder()
        .max_size(queryplan.query_data.len() as u32)
        .build(manager)
        .expect("Could not create a connection");

    let schema = source.get_schema_of(queries.clone().get(0).unwrap());

    let arrays: Vec<Vec<ArrayRef>> = queryplan
        .query_data
        .into_par_iter()
        .map(|query| {
            let mut client = pool.get().unwrap();

            let rows: RowIter = client
                .query_raw::<_, bool, _>(query.as_str(), vec![])
                .expect("Query failed");

            let mut builders: Vec<Box<dyn ArrayBuilder>> = get_arrow_builders(schema.clone());

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
            // return make_record_batch(
            //     arrays,
            //     schema.columns.iter().map(|col| col.name.clone()).collect()
            // );
        })
        .collect::<Vec<_>>();
    metadata.print_step("Finishing loading data");
    log_memory_with_message("Data is loaded");

    (arrays, schema)

}
