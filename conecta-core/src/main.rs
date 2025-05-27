use tokio_postgres::{NoTls, Error};
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Error> {
    use std::time::Instant;
    let now = Instant::now();
    // Set up first connection
    let (client1, connection1) =
        tokio_postgres::connect("postgres://postgres:postgres@192.168.88.251:5400/postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection1.await {
            eprintln!("connection1 error: {}", e);
        }
    });

    // Set up second connection
    let (client2, connection2) =
        tokio_postgres::connect("postgres://postgres:postgres@192.168.88.251:5400/postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection2.await {
            eprintln!("connection2 error: {}", e);
        }
    });

    let (client3, connection2) =
        tokio_postgres::connect("postgres://postgres:postgres@192.168.88.251:5400/postgres", NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection2.await {
            eprintln!("connection2 error: {}", e);
        }
    });

    // Run both queries in parallel tasks
    let query1 = task::spawn(async move {
        let rows = client1.query("select * from lineitem where l_orderkey >= 0 and l_orderkey < 399989", &[]).await?;
        println!("Query 1 result: {:?}", rows.len());
        Ok::<_, Error>(())
    });

    let query2 = task::spawn(async move {
        let rows = client2.query("select * from lineitem where l_orderkey >= 399989 and l_orderkey < 799978", &[]).await?;
        println!("Query 2 result: {:?}", rows.len());
        Ok::<_, Error>(())
    });

    let query3 = task::spawn(async move {
        let rows = client3.query("select * from lineitem where l_orderkey > 799978", &[]).await?;
        println!("Query 3 result: {:?}", rows.len());
        Ok::<_, Error>(())
    });

    // Wait for both queries to finish
    query1.await.unwrap()?;

    let elapsed = now.elapsed();

    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}
