use crate::partition::PartitionConfig;
use crate::source::Source;

#[derive(Debug)]
pub enum NeededMetadataFromSource {
    Count,
    CountAndMinMax,
}
pub fn create_metadata(
    source: &Box<dyn Source>,
    partition_config: PartitionConfig,
) -> Metadata {
    let query_metadata: Vec<QueryMetadata> = partition_config.queries
        .iter()
        .map(|query| {
            source.fetch_query_metadata(query,
                                        partition_config.partition_on.as_deref(),
                                        &partition_config.needed_metadata_from_source,
                                        partition_config.partition_range)
        })
        .collect();

    let metadata = Metadata {
        queries: query_metadata,
        partition_config,
    };
    metadata
}

/// Represents the metadata that the `Source`s will request before creating partitions.
#[derive(Debug)]
pub struct Metadata {
    pub queries: Vec<QueryMetadata>,
    pub partition_config: PartitionConfig,
}

#[derive(Debug)]
pub struct QueryMetadata {
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,

    /// Total count of rows of the original query, obtained with `metadata_query`
    pub count: i64,

    /// The query that will be used to get metadata, count and/or min & max.
    pub metadata_query: String,

    /// The query that was originally passed by the user.
    pub query: String,

    /// The query that will be used to fetch the data, with partition included if requested.
    pub query_data: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_conn_ok() {}
}
