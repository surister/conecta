use crate::source::Source;

pub enum NeededMetadataFromSource {
    Count,
    CountAndMinMax,
}
pub fn create_metadata<'a>(
    source: Box<dyn Source>,
    queries: Vec<&'a str>,
    partition_on: Option<&'a str>,
    partition_range: &[u32],
    partition_num: Option<u16>,
) -> Metadata<'a> {
    if (partition_num.is_some() || partition_on.is_some() || partition_range.len() > 0)
        && queries.len() > 1
    {
        panic!(
            "Double partition scheme error: You have passed several queries\
            (user defined partition) and one or some partition_* option \
            (conecta's defined partition), passing these are not compatible.\
            Read more at: [TODO PIECE OF DOC that explains what's happening nad how to fix it]"
        )
    }

    if partition_num.is_some() && partition_on.is_none() {
        panic!(
            "You passed partition_num={}, but partition_on is None, hint: \
            pass a column name.",
            partition_num.unwrap()
        )
    }

    let query_metadata = queries
        .into_iter()
        .map(|query| {
            let needed_metadata = {
                if partition_range.is_empty() && partition_num.is_some() && partition_on.is_some() {
                    NeededMetadataFromSource::CountAndMinMax
                } else {
                    NeededMetadataFromSource::Count
                }
            };
            source.request_metadata(query, partition_on, needed_metadata)
        })
        .collect();

    let metadata = Metadata {
        queries: query_metadata,
        partition_on,
    };
    metadata
}

/// Represents the metadata that the `Source`s will request before creating partitions.
#[derive(Debug)]
pub struct Metadata<'a> {
    pub queries: Vec<QueryMetadata>,
    pub partition_on: Option<&'a str>,
}

#[derive(Debug)]
pub struct QueryMetadata {
    pub max_value: Option<i64>,
    pub min_value: Option<i64>,
    /// Total count of rows that will be requested across all partitions.
    pub count: i64,
    pub query: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_conn_ok() {}
}
