
/// Represents a Partitioned query. A query not partitioned by the user nor `strategy`
/// will still be represented by one partition, that will contain all the data.
#[derive(Debug)]
pub struct Partition {
    /// The partitioned query, e.g. `select * from lineitem where l_orderkey > 1 and l_orderkey < 1000`
    pub query: String
}