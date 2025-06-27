use crate::metadata::{NeededMetadataFromSource, QueryPartitioningMode};
use crate::source::Source;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct PartitionConfig {
    pub queries: Vec<String>,
    pub partition_on: Option<String>,
    pub partition_num: Option<u16>,
    pub partition_range: Option<(i64, i64)>,
    pub needed_metadata_from_source: NeededMetadataFromSource,
    pub query_partition_mode: QueryPartitioningMode,
}

impl PartitionConfig {
    pub fn new(
        queries: Vec<String>,
        partition_on: Option<String>,
        partition_num: Option<u16>,
        partition_range: Option<(i64, i64)>,
    ) -> Self {
        if queries.is_empty() {
            panic!("must pass some queries!")
        }

        if (partition_num.is_some() || partition_on.is_some() || partition_range.is_some())
            && queries.len() > 1
        {
            panic!(
                "Double partition scheme error: You have passed several queries
                (user defined partition) and one or some partition_* option 
                (conecta's defined partition), passing these are not compatible.
                Read more at: [TODO PIECE OF DOC that explains what's happening 
                nad how to fix it]"
            )
        }

        if partition_num.is_some() && partition_on.is_none() {
            panic!(
                "You passed partition_num={}, but partition_on is None, hint:
                pass a column name.",
                partition_num.unwrap()
            )
        }

        if !partition_range.is_none() && partition_on.is_none() {
            panic!("You passed a partition_range but did not specified a partition_on.")
        }

        // Check that min/max values are valid.
        if let Some((min, max)) = partition_range {
            if min >= max {
                panic!(
                    "partition_range is (min, max) but min is \
                    not smaller than max; min={:?}, max={:?}",
                    min, max
                );
            }
        }

        let needed_metadata_from_source = {
            if partition_range.is_none() && partition_num.is_some() && partition_on.is_some() {
                NeededMetadataFromSource::CountAndMinMax
            } else {
                NeededMetadataFromSource::Count
            }
        };

        let partition_mode = match (
            partition_on.is_some(),
            partition_num.is_some(),
            queries.len(),
        ) {
            (true, true, 1) => QueryPartitioningMode::OnePartitionedQuery,
            (_, _, n) if n > 1 => QueryPartitioningMode::PartitionedQueries,
            _ => QueryPartitioningMode::OneUnpartitionedQuery,
        };

        PartitionConfig {
            queries,
            partition_range,
            partition_num,
            partition_on,
            needed_metadata_from_source,
            query_partition_mode: partition_mode,
        }
    }
}

fn bounds(min: i64, max: i64, n: usize) -> Vec<(i64, i64)> {
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

pub fn created_bounded_queries(
    source: &Box<dyn Source>,
    query: &str,
    partition_on: &str,
    partition_num: u16,
    min: i64,
    max: i64,
) -> Vec<String> {
    let mut data_queries: Vec<String> = Vec::with_capacity(partition_num as usize);
    for bound in bounds(min, max, partition_num as usize) {
        data_queries.push(source.wrap_query_with_bounds(query, partition_on, bound));
    }
    data_queries
}

#[cfg(test)]
mod create_bound_tests {
    use super::*;
    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_even_partition() {
            let result = bounds(0, 10, 2);
            assert_eq!(result, vec![(0, 5), (5, 10)]);
        }

        #[test]
        fn test_uneven_partition() {
            let result = bounds(0, 10, 3);
            assert_eq!(result, vec![(0, 3), (3, 6), (7, 10)]);
        }

        #[test]
        fn test_single_partition() {
            let result = bounds(5, 10, 1);
            assert_eq!(result, vec![(5, 10)]);
        }

        #[test]
        #[should_panic(expected = "min must be less than max")]
        fn test_invalid_range() {
            bounds(10, 5, 3);
        }

        #[test]
        #[should_panic(expected = "n must be greater than 0")]
        fn test_zero_partitions() {
            bounds(0, 10, 0);
        }
    }
}
#[cfg(test)]
mod config_partition_tests {
    use super::*;

    #[test]
    fn test_valid_single_query_no_partition() {
        let config =
            PartitionConfig::new(vec!["SELECT * FROM lineitem".to_string()], None, None, None);
        assert_eq!(config.queries.len(), 1);
        assert_eq!(
            config.needed_metadata_from_source,
            NeededMetadataFromSource::Count
        );
        assert_eq!(
            config.query_partition_mode,
            QueryPartitioningMode::OneUnpartitionedQuery
        );
    }

    #[test]
    fn test_valid_partition_on_with_num() {
        let column = Some("l_orderkey".to_string());
        let config = PartitionConfig::new(
            vec!["SELECT * FROM lineitem".to_string()],
            column.clone(),
            Some(4),
            None,
        );
        assert_eq!(config.partition_num, Some(4));
        assert_eq!(config.partition_on, column);
        assert_eq!(
            config.needed_metadata_from_source,
            NeededMetadataFromSource::CountAndMinMax
        );
        assert_eq!(
            config.query_partition_mode,
            QueryPartitioningMode::OnePartitionedQuery
        );
    }

    #[test]
    fn test_valid_partition_on_with_range() {
        let config = PartitionConfig::new(
            vec!["SELECT * FROM lineitem".to_string()],
            Some("l_orderkey".to_string()),
            None,
            Some((0, 100)),
        );
        assert_eq!(config.partition_range, Some((0, 100)));
        assert_eq!(
            config.needed_metadata_from_source,
            NeededMetadataFromSource::Count
        );
        assert_eq!(
            config.query_partition_mode,
            QueryPartitioningMode::OneUnpartitionedQuery
        );
    }

    #[test]
    #[should_panic(expected = "Double partition scheme error")]
    fn test_double_partition_scheme_panics() {
        PartitionConfig::new(
            vec!["SELECT * FROM a".to_string(), "SELECT * FROM b".to_string()],
            Some("id".to_string()),
            None,
            None,
        );
    }

    #[test]
    #[should_panic(expected = "You passed partition_num")]
    fn test_partition_num_without_partition_on_panics() {
        PartitionConfig::new(
            vec!["SELECT * FROM lineitem".to_string()],
            None,
            Some(2),
            None,
        );
    }
    #[test]
    #[should_panic(expected = "partition_range but did not specified a partition_on")]
    fn test_partition_range_without_partition_on_panics() {
        PartitionConfig::new(
            vec!["SELECT * FROM lineitem".to_string()],
            None,
            None,
            Some((0, 100)),
        );
    }

    #[test]
    #[should_panic(expected = "min is not smaller than max")]
    fn test_partition_range_invalid_order_panics() {
        PartitionConfig::new(
            vec!["SELECT * FROM data".to_string()],
            Some("value".to_string()),
            None,
            Some((100, 100)),
        );
    }
}
