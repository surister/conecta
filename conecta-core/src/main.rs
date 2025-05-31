use conecta_core::logger::log_memory;

use conecta_core::source::get_source;

use conecta_core::metadata::create_metadata;

use conecta_core::partition::create_partitions;
use tokio_postgres::Error;

enum QueryPartitioningMode {
    /// A single unpartitioned query provided by the user.
    /// `partition_num` is `None`.
    OneUnpartitionedQuery,

    /// A single query that should be partitioned by the system.
    /// Both `partition_num` and `partition_column` must be set.
    OnePartitionedQuery,

    /// Multiple queries provided by the user, already partitioned.
    PartitionedQueries,
}

fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();
    let connection_string = "postgres://pg:pg@localhost:5432/postgres";

    // VARIABLES FROM USER
    let query: Vec<&str> = vec!["select * from lineitem"];

    let partition_on = None;
    let partition_range: Vec<i64> = vec![];
    let partition_num: Option<u16> = None;

    /*    let query: Vec<&str> = vec![
        "select * from lineitem where l_orderkey > 0 and l_orderkey < 20",
        "select * from lineitem where l_orderkey > 20 and l_orderkey < 50",
    ];*/

    /*    let partition_on = None;
    let partition_range: Vec<i64> = vec![];
    let partition_num: Option<u16> = None;*/

    let source = get_source(connection_string, None);
    let metadata = create_metadata(
        &source,
        query,
        partition_on,
        &*partition_range,
        partition_num,
    );

    let metadata = create_partitions(metadata, source);

    println!("\n\n{:#?}", metadata);

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
