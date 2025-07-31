use crate::partition::{created_bounded_queries, PartitionConfig};
use crate::source::Source;
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize)]
pub enum NeededMetadataFromSource {
    Count,
    MinMax,
    CountAndMinMax,
    // The user has already provided both, or one but the destination does not need it.
    None,
}

#[derive(Debug, PartialEq, Serialize)]
pub enum QueryPartitioningMode {
    /// A single unpartitioned query provided by the user.
    OneUnpartitionedQuery,

    /// A single query that should be partitioned by the system.
    /// Both `partition_num` and `partition_column` must be also given by the user.
    OnePartitionedQuery,

    /// Multiple queries provided by the user, already partitioned.
    PartitionedQueries,
}

/// Creates a partition plan given the `Source` and the user configuration `PartitionConfig`.
pub fn create_partition_plan(
    source: &Box<dyn Source>,
    partition_config: PartitionConfig,
) -> PartitionPlan {
    let query_data: Vec<String>;

    let query = match partition_config.queries.len() {
        1 => partition_config.queries.get(0).unwrap(),

        // We always merge the metadata queries into one, to avoid the overhead of sending
        // several queries.
        _ => &source.merge_queries(&partition_config.queries),
    };
    let (min, max, count, metadata_query) = source.fetch_metadata(
        query,
        partition_config.partition_on.as_deref(),
        &partition_config.needed_metadata_from_source,
        partition_config.partition_range,
    );

    // If the user didn't specify partition_range, we use the one fetched from fetch_metadata, this
    // method might actually not fetch min/max if it is unnecessary, hence the `Option<i64>`.
    let (min_value, max_value) = match partition_config.partition_range {
        Some((a, b)) => (Some(a), Some(b)),
        None => (min, max),
    };

    match partition_config.query_partition_mode {
        QueryPartitioningMode::OnePartitionedQuery => {
            // Create the bounded queries.
            query_data = created_bounded_queries(
                source,
                partition_config.queries[0].as_str(),
                &partition_config.partition_on.clone().unwrap(),
                partition_config.partition_num.unwrap(),
                min_value.expect("should have a valid min at this point"),
                max_value.expect("should have a valid max at this point"),
            )
        }
        // If we don't need to create any query (by partitioning it), we just set query_data
        // to whatever query(s) the user provided.
        _ => query_data = Vec::from(partition_config.queries.clone()),
    }

    let partition_plan = PartitionPlan {
        min_value,
        max_value,
        count,
        metadata_query,
        query_data,
        partition_config,
    };
    partition_plan
}

/// Represents the metadata that the `Source`s will request before creating partitions.

#[derive(Debug, Serialize)]
pub struct PartitionPlan {
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,

    /// Total count of rows, obtained with `metadata_query`
    pub count: i64,

    /// The query that will be used to get metadata, count and/or min & max.
    pub metadata_query: String,

    /// The query that will be used to fetch the data, with partition included if requested.
    pub query_data: Vec<String>,

    /// The configuration used to generate the QueryPlan. It is validated users input.
    pub partition_config: PartitionConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    #[derive(Debug)]
    struct DummySource;

    impl Source for DummySource {
        fn get_conn_string(&self) -> String {
            "someconn".to_string()
        }
        fn get_metadata_query(
            &self,
            query: &str,
            column: Option<&str>,
            needed_metadata_from_source: &NeededMetadataFromSource,
            partition_range: Option<(i64, i64)>,
        ) -> String {
            "some_metadata_query".to_string()
        }
        fn get_table_name(&self, query: &str) -> String {
            "some_table_name".to_string()
        }
        fn get_schema_query(&self, original_query: &str) -> String {
            "some_schema_query".to_string()
        }
        fn fetch_metadata(
            &self,
            query: &str,
            column: Option<&str>,
            needed_metadata: &NeededMetadataFromSource,
            partition_range: Option<(i64, i64)>,
        ) -> (Option<i64>, Option<i64>, i64, String) {
            (
                Some(1i64),
                Some(10i64),
                10,
                "some_metadata_query".to_string(),
            )
        }
        fn wrap_query_with_bounds(
            &self,
            query: &str,
            column: &str,
            bounds: (i64, i64),
            is_last: bool,
        ) -> String {
            "wrapped()".to_string()
        }
        fn merge_queries(&self, queries: &Vec<String>) -> String {
            "".to_string()
        }
        fn send_query(&self, query: &str) {
            todo!()
        }
        fn validate(&self) {
            todo!()
        }

        fn get_schema_of(&self, query: &str) -> Schema {
            todo!()
        }
    }

    #[test]
    fn test_create_query_plan_one_partitioned_query() {
        let source: Box<dyn Source> = Box::new(DummySource);
        let partitions_num = Some(4);
        let partition_config = PartitionConfig::new(
            vec!["select * from l_orderkey".to_string()],
            Some("col".to_string()),
            partitions_num,
            None,
        );

        let query_plan = create_partition_plan(&source, partition_config);
        assert_eq!(query_plan.min_value, Some(1));
        assert_eq!(query_plan.max_value, Some(10));
        assert_eq!(query_plan.count, 10);
        assert_eq!(
            query_plan.query_data.len(),
            partitions_num.unwrap() as usize
        )
    }

    #[test]
    fn test_create_query_plan_one_partitioned_query_ranged() {
        let source: Box<dyn Source> = Box::new(DummySource);
        let partitions_num = Some(4);
        let partition_range = Some((10i64, 10000i64));
        let partition_config = PartitionConfig::new(
            vec!["select * from l_orderkey".to_string()],
            Some("col".to_string()),
            partitions_num,
            partition_range,
        );

        let query_plan = create_partition_plan(&source, partition_config);
        assert_eq!(query_plan.min_value, Some(partition_range.unwrap().0));
        assert_eq!(query_plan.max_value, Some(partition_range.unwrap().1));
        assert_eq!(query_plan.count, 10);
        assert_eq!(
            query_plan.query_data.len() as i16,
            partitions_num.unwrap() as i16
        )
    }
    #[test]
    fn test_create_query_plan_unpartitioned_single_query() {
        let source: Box<dyn Source> = Box::new(DummySource);
        let partitions_num = None;
        let partition_range = None;
        let partition_config = PartitionConfig::new(
            vec!["select * from l_orderkey".to_string()],
            Some("col".to_string()),
            partitions_num,
            partition_range,
        );

        let query_plan = create_partition_plan(&source, partition_config);
        assert_eq!(query_plan.min_value, Some(1));
        assert_eq!(query_plan.max_value, Some(10));
        assert_eq!(query_plan.count, 10);
        assert_eq!(query_plan.query_data.len(), 1)
    }
    #[test]
    fn test_create_query_plan_unpartitioned_single_query_ranged() {
        let source: Box<dyn Source> = Box::new(DummySource);
        let partitions_num = None;
        let partition_range = Some((10i64, 10000i64));
        let partition_config = PartitionConfig::new(
            vec!["select * from l_orderkey".to_string()],
            Some("col".to_string()),
            partitions_num,
            partition_range,
        );

        let query_plan = create_partition_plan(&source, partition_config);
        println!("{:#?}", query_plan);
        assert_eq!(query_plan.min_value, Some(partition_range.unwrap().0));
        assert_eq!(query_plan.max_value, Some(partition_range.unwrap().1));
        assert_eq!(query_plan.count, 10);
        assert_eq!(query_plan.query_data.len(), 1)
    }

    #[test]
    fn test_create_query_plan_already_partitioned_query() {
        let source: Box<dyn Source> = Box::new(DummySource);
        let partitions_num = None;
        let partition_range = None;
        let partition_config = PartitionConfig::new(
            vec![
                "select * from l_orderkey where a".to_string(),
                "select * from l_orderkey where b".to_string(),
            ],
            None,
            partitions_num,
            partition_range,
        );

        let query_plan = create_partition_plan(&source, partition_config);
        assert_eq!(query_plan.min_value, Some(1));
        assert_eq!(query_plan.max_value, Some(10));
        assert_eq!(query_plan.count, 10);
        assert_eq!(query_plan.query_data.len(), 2)
    }
}
