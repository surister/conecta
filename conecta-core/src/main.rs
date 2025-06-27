use arrow::array::{ArrayBuilder, ArrayRef, RecordBatch};
use chrono::Datelike;
use conecta_core::logger::{log_memory, log_memory_with_label, log_peak_memory};
use conecta_core::metadata::create_partition_plan;
use conecta_core::partition::{created_bounded_queries, PartitionConfig};
use conecta_core::source::get_source;
use r2d2_postgres::postgres::Client;
use r2d2_postgres::{postgres, r2d2, PostgresConnectionManager};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::fmt::format;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::*;
use conecta_core::destination::{ArrowDestination, Destination};
use conecta_core::schema::NativeType;
use tokio_postgres::types::IsNull::No;
use tokio_postgres::{Error, NoTls};

use arrow::error::{ArrowError, Result as ArrowResult};
use r2d2_postgres::r2d2::{ManageConnection, Pool, PooledConnection};
use sqlparser::ast::DataType::Float32;

fn query_to_record_batch(
    pool: Pool<PostgresConnectionManager<NoTls>>,
    query: &str,
) -> Result<RecordBatch, ArrowError> {
    let mut client: PooledConnection<PostgresConnectionManager<NoTls>> = pool.get().unwrap();
    let rows = client.query(query, &[]).expect("Query failed");

    let columns = rows[0].columns();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());

    for (col_idx, col) in columns.iter().enumerate() {
        let name = col.name().to_string();
        let col_type = col.type_();
        let mut nulls = false;
        
        let array: ArrayRef = match *col_type {
            postgres::types::Type::NUMERIC => {
                let mut builder = Float32Builder::new();
                for row in &rows {
                    match row.try_get::<usize, f32>(col_idx) {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            postgres::types::Type::INT4 => {
                let mut builder = Int32Builder::new();
                for row in &rows {
                    match row.try_get::<usize, i32>(col_idx) {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            postgres::types::Type::FLOAT8 => {
                let mut builder = Float64Builder::new();
                for row in &rows {
                    match row.try_get::<usize, f64>(col_idx) {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            postgres::types::Type::VARCHAR
            | postgres::types::Type::TEXT
            | postgres::types::Type::BPCHAR => {
                let mut builder = StringBuilder::new();
                for row in &rows {
                    match row.try_get::<usize, String>(col_idx) {
                        Ok(v) => builder.append_value(&v),
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            postgres::types::Type::DATE => {
                use chrono::NaiveDate;
                let mut builder = Date32Builder::new();
                for row in &rows {
                    match row.try_get::<usize, NaiveDate>(col_idx) {
                        Ok(v) => {
                            let days = v.num_days_from_ce()
                                - NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .unwrap()
                                    .num_days_from_ce();
                            builder.append_value(days);
                        }
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            postgres::types::Type::BOOL => {
                let mut builder = BooleanBuilder::new();
                for row in &rows {
                    match row.try_get::<usize, bool>(col_idx) {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            builder.append_null();
                            nulls = true;
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Unsupported type
                panic!("bad {}", col_type)
            }
        };

        let arrow_data_type = array.data_type().clone();
        fields.push(Field::new(&name, arrow_data_type, nulls));
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
}

fn format_number(number: usize) -> String {
    format!("{:?}", number)
        .chars()
        .collect::<Vec<_>>()
        .chunks(3)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("_")
        .chars()
        .collect::<String>()
}
fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();

    let connection_string = "postgres://postgres:postgres@192.168.88.251:5400/postgres";
    let manager = PostgresConnectionManager::new(connection_string.parse()?, NoTls);

    // VARIABLES FROM USER
    let queries: Vec<String> = vec!["select l_orderkey from lineitem".to_string()];

    let partition_on = Some("l_orderkey".to_string());
    let partition_range: Option<(i64, i64)> = None;
    let partition_num: Option<u16> = Some(4);

    /*    validate_partition_parameters();*/

    /*    let query: Vec<&str> = vec![
        "select * from lineitem where l_orderkey > 0 and l_orderkey < 20",
        "select * from lineitem where l_orderkey > 20 and l_orderkey < 50",
    ];*/

    /*    let partition_on = None;
    let partition_range: Vec<i64> = vec![];
    let partition_num: Option<u16> = None;*/
    let partition_config =
        PartitionConfig::new(queries, partition_on, partition_num, partition_range);

    let source = get_source(connection_string, None);
    let queryplan = create_partition_plan(&source, partition_config);

    let pool = r2d2::Pool::builder()
        .max_size(queryplan.query_data.len() as u32)
        .build(manager)
        .unwrap();
    let s = Arc::new(source.get_schema_of("select * from lineitem"));
    let ncols = s.columns.iter().len();
    let c: Vec<RecordBatch> = queryplan
        .query_data
        .into_par_iter()
        .map(|x| query_to_record_batch(pool.clone(), &x).expect("TODO: panic message"))
        .collect();
    println!("{:?}", c);
    // let t: Vec<Vec<_>> = queryplan.query_data.into_par_iter().map(|x|{
    //     let pool = pool.clone();
    //     let mut client = pool.get().unwrap();
    //     let result = client.query(&x, &[]).unwrap();
    //     println!("{:?}", );
    //     for i in 0..s.columns.iter().len(){
    //
    //     }
    //     let stuff: Vec<i32> = result.into_iter().map(|r|{
    //         r.try_get(8).expect("TODO: panic message")
    //     }).collect();
    //     stuff
    // }).collect();
    // println!("{:?}", t[0].len());
    // println!("{:?}", t.len());

    let d = ArrowDestination {};

    log_memory_with_label(format!("allocating {}ˣ{}", 2, format_number(1)).as_str());

    log_memory();
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    log_peak_memory();
    Ok(())
}
