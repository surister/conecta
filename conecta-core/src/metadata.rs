use crate::source::Source;

pub enum NeededMetadataFromSource {
    Count,
    CountAndMinMax,
}
pub fn create_metadata<'a>(
    source: &Box<dyn Source>,
    queries: Vec<&'a str>,
    partition_column: Option<&'a str>,
    partition_range: &[i64],
    partition_num: Option<u16>,
) -> Metadata<'a> {
    let query_metadata = queries
        .into_iter()
        .map(|query| {
            let needed_metadata = {
                if partition_range.is_empty()
                    && partition_num.is_some()
                    && partition_column.is_some()
                {
                    NeededMetadataFromSource::CountAndMinMax
                } else {
                    NeededMetadataFromSource::Count
                }
            };
            source.fetch_query_metadata(query, partition_column, needed_metadata, partition_range)
        })
        .collect();

    let metadata = Metadata {
        queries: query_metadata,
        partition_column,
        partition_range: Vec::from(partition_range),
        partition_num,
    };
    metadata
}

/// Represents the metadata that the `Source`s will request before creating partitions.
#[derive(Debug)]
pub struct Metadata<'a> {
    pub queries: Vec<QueryMetadata>,
    pub partition_column: Option<&'a str>,
    pub partition_range: Vec<i64>,
    pub partition_num: Option<u16>,
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
