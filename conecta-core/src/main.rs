use tokio_postgres::{Error};
use conecta_core::logger::log_memory;
use conecta_core::source::{get_source, parse_conn};
use conecta_core::partition::{Partition};
#[tokio::main]
async fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();

    env_logger::init();
    log_memory();

    let connection_string = "postgres://postgres:postgres@192.168.88.251:5400/postgres";
    let source = get_source(connection_string);

    // let partitions: Vec<Partition> = source.create_partitions();
    let partitions = vec![
        Partition {
            query: String::from("select * from lineitem where l_orderkey > 1 and l_orderkey < 1000"),
        },
        Partition {
            query: String::from("select * from lineitem where l_orderkey > 1000 and l_orderkey < 2000"),
        }
    ];
    // let pool = source.get_pool();
    println!("{:?}", source);
    println!("{:?}", partitions);


    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}
