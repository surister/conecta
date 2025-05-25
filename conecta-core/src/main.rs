mod logger;

use std::sync::Arc;
use arrow::array::{Int32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio_postgres::{Error, NoTls};

use crate::logger::{log_memory_with_label, log_peak_memory};

static URI: &str = "postgres://postgres:postgres@192.168.88.251:5400/postgres";
static QUERY: &str = "select * from lineitem";

#[tokio::main]
async fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();

    log_memory_with_label("Pre-allocation of Arrow builders");

    // Pre allocate memory with pre-known counts.
    let mut builder = Int32Builder::with_capacity(1199969);
    let mut builder2 = Int32Builder::with_capacity(1199969);

    log_memory_with_label("Post allocation of Arrow builders");

    let (client, connection) = tokio_postgres::connect(URI, NoTls).await?;
    
    tokio::spawn(connection);

    let rows = client.query(QUERY, &[]).await?;
    
    log_memory_with_label("Data is fetched");
    
    let mut buff1 = Vec::with_capacity(1199969);
    let mut buff2 = Vec::with_capacity(1199969);
    
    for row in rows {
        let value: i32 = row.get(0);
        let value2: i32 = row.get(1);
        buff1.push(value);
        buff2.push(value2);
    }
    
    log_memory_with_label("Data is deserialized into buffers");
    
    builder.append_slice(&buff1);
    builder2.append_slice(&buff2);

    let arrow_array = builder.finish();
    let arrow_array2 = builder2.finish();

    let schema = Schema::new(vec![
        Field::new("your_column_name", DataType::Int32, false),
        Field::new("col2", DataType::Int32, false)
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(arrow_array), Arc::new(arrow_array2)],
    ).unwrap();
    
    println!("{:?}", record_batch.num_rows());
    
    log_memory_with_label("Record batch is created and freed");
    
    let elapsed = now.elapsed();
    
    println!("Elapsed: {:.2?}", elapsed);
    log_peak_memory();
    Ok(())
}