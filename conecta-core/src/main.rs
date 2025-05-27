use conecta_core::logger::log_memory;
use conecta_core::partition::Partition;
use conecta_core::source::get_source;

use arrow::util::pretty::print_columns;
use conecta_core::metadata::create_metadata;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use std::thread;
use tokio_postgres::types::IsNull::No;
use tokio_postgres::{Error, NoTls};

fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();
    let connection_string = "postgres://postgres:postgres@192.168.88.251:5400/postgres";

    // VARIABLES FROM USER
    let query: Vec<&str> = vec![
        "select * from lineitem",
        // "select * from lineitem where l_orderkey > 20 and l_orderkey < 50",
    ];

    let partition_on = Some("l_orderkey");
    let partition_range: Vec<u32> = vec![1, 10];
    let partition_num: Option<u16> = None;

    let source = get_source(connection_string, None);
    let metadata = create_metadata(
        source,
        query,
        partition_on,
        &*partition_range,
        partition_num,
    );

    println!("{:#?}", metadata);
    print!("{:?}", i8::MAX);
    // let client = src.get_client();
    // client.query(src.get_metadata_query());
    // match srctype {
    //     postgres => {},
    //     sqlite => {}
    // }
    // let partitions: Vec<Partition> = source.create_partitions();
    // partitions.par_iter(|partition| (source.run_partition(partition)))

    // let partitions = create_partitions(source, ...);
    /* source.run_partition(partition);
    run_partition(source, partition);*/

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}
