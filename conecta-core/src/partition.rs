use crate::metadata::{Metadata, QueryMetadata};
use crate::source::Source;

/// Represents a Partitioned query. A query not partitioned by the user nor `strategy`
/// will still be represented by one partition, that will contain all the data.
#[derive(Debug)]
pub struct PartitionPlan {
    /// The partitioned query, e.g. `select * from lineitem where l_orderkey > 1 and l_orderkey < 1000`
    pub query: String,
    pub query_metadata: QueryMetadata,
}
fn create_bounds(min: i64, max: i64, n: usize) -> Vec<(i64, i64)> {
    assert!(min < max, "min must be less than max");
    assert!(n > 0, "n must be greater than 0");

    let mut bounds: Vec<(i64, i64)> = Vec::with_capacity(n);

    let range = (max - min) as f64;
    let step = range / n as f64;

    for i in 0..n {
        let start = (step * i as f64 + min as f64).round() as i64;
        let mut stop = (start as f64 + step).round() as i64;
        if i == n - 1 {
            stop = max; 
            // last one we set to max, otherwise we will be a bit off due to rounding
            // if the total count and n are not divisible, the last partition will have
            // the extra row.
        }
        bounds.push((start, stop));
    }

    bounds
}
pub fn create_partitions(mut metadata: Metadata, source: Box<dyn Source>) -> Metadata {
    println!("hmmm");
    match metadata.queries.len() {
        0 => panic!(
            "Trying to create partition but there are/is no query, something went very wrong."
        ),
        1 => {
            if let Some(column) = metadata.partition_column {
                for i in create_bounds(
                    metadata.queries[0].min_value.unwrap(),
                    metadata.queries[0].max_value.unwrap(),
                    metadata.partition_num.unwrap() as usize,
                ) {
                    let mut queries = Vec::from(metadata.queries[0].query_data.clone());
                    queries.push(source.wrap_query_with_bounds(
                        metadata.queries[0].query.as_str(),
                        column,
                        i,
                    ));
                    metadata.queries[0].query_data = queries;
                }
                for i in 0..metadata
                    .partition_num
                    .expect("metadata is expected to have partition_num at this point")
                {
                    println!("query: {i}",);
                }
            } else {
                metadata.queries[0].query_data = vec![metadata.queries[0].query.clone()];
            }
        }

        _ => {
            println!("more than 1 query by the user, we don't do shit");
        }
    }

    metadata
}
