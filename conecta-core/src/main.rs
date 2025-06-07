use arrow::array::ArrayBuilder;
use conecta_core::logger::{log_memory, log_peak_memory};
use conecta_core::metadata::create_queryplan;
use conecta_core::partition::{create_query_partitions, PartitionConfig};
use conecta_core::source::get_source;

use tokio_postgres::Error;
use conecta_core::destination::{ArrowDestination, Destination};
use conecta_core::schema::NativeType;

fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();

    let connection_string = "postgres://pg:pg@localhost:5432/postgres";

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
    let s = source.get_schema_of("select * from lineitem");
    println!("{:#?}", s);
    // println!("\n\n{:#?}", queryplan);
    log_memory();
    println!("allocated?");
    let d = ArrowDestination {};
    let builders = d.make_builders(vec![NativeType::I32], 10_000_000);
    for builder in builders.iter() {
        builder.append_nulls(10_000_000)
   
    }
    log_memory();
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    log_peak_memory();
    Ok(())
}
