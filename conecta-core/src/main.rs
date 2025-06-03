use conecta_core::logger::log_memory;
use conecta_core::metadata::create_queryplan;
use conecta_core::partition::{create_query_partitions, PartitionConfig};
use conecta_core::source::get_source;

use tokio_postgres::Error;

fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();

    let connection_string = "postgres://postgres:postgres@192.168.88.251:5400/postgres";

    // VARIABLES FROM USER
    let queries: Vec<String> = vec!["select * from lineitem".to_string()];

    let partition_on = Some("l_orderkey".to_string());
    let partition_range: Option<(i64, i64)> = Some((1i64, 10000i64));
    let partition_num: Option<u16> = Some(6);

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
    let queryplan = create_queryplan(&source, partition_config);
    source.get_schema_of("select * from lineitem");

    // println!("\n\n{:#?}", queryplan);

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
