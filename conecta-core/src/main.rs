mod logger;

use std::sync::Arc;
use arrow::array::{UInt32Array, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio_postgres::{Error, NoTls};

use crate::logger::{log_memory, log_peak_memory};

static URI: &str = "postgres://postgres:postgres@192.168.88.251:5400/postgres";
static QUERY: &str = "select * from lineitem";

#[tokio::main]
async fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();
    env_logger::init();
    log_memory();
    
    // Pre allocate memory with pre-known counts.
    let mut builder = UInt32Builder::with_capacity(1199969);
    let mut builder2 = UInt32Builder::with_capacity(1199969);
    
    let (client, connection) = tokio_postgres::connect(URI, NoTls).await?;


    
    log_memory();
    
    tokio::spawn(connection);
    
    let rows = client.query(QUERY, &[]).await?;
    
    for row in rows {
        let value: i32 = row.get(0);
        let value2: i32 = row.get(1);
        
        if value >= 0 {
            builder.append_value(value as u32);
        } else {
            builder.append_null();
        }
        if value2 >= 0 {
            builder2.append_value(value as u32);
        } else {
            builder2.append_null();
        }
    }
    
    // for row in rows {
    //     let value: i32 = row.get(0);
    //     let converted_value = if value >= 0 {
    //         Some(value as u32) // Safe conversion
    //     } else {
    //         None
    //     };
    //     column_values.push(converted_value);
    // }
    // let arrow_values: Vec<u32> = column_values.into_iter().filter_map(|v| v).collect(); // filter None values
    // let arrow_array = UInt32Array::from(arrow_values);
    
    let arrow_array = builder.finish();
    let arrow_array2 = builder2.finish();
    
    let schema = Schema::new(vec![
        Field::new("your_column_name", DataType::UInt32, false),
        Field::new("col2", DataType::UInt32, false)
    ]);
    
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(arrow_array), Arc::new(arrow_array2)],
    ).unwrap();
 
    log_memory();
    log_peak_memory();
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}