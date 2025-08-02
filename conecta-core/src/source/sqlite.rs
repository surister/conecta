use crate::metadata::NeededMetadataFromSource;
use crate::schema::{NativeType, Schema};
use crate::source::source::Source;
use arrow::array::ArrayRef;
use postgres::{Column, NoTls, Row};
use r2d2_postgres::PostgresConnectionManager;
use r2d2_postgres::r2d2::PooledConnection;

#[derive(Debug)]
pub struct SqliteSource {
    pub conn_string: String,
}

impl Source for SqliteSource {
    fn get_conn_string(&self) -> String {
        self.conn_string.clone()
    }
    fn wrap_query_with_bounds(
        &self,
        query: &str,
        column: &str,
        bounds: (i64, i64),
        is_last: bool,
    ) -> String {
        todo!()
    }

    fn merge_queries(&self, queries: &Vec<String>) -> String {
        todo!()
    }

    fn get_schema_query(&self, original_query: &str) -> String {
        todo!()
    }

    fn get_table_name(&self, query: &str) -> String {
        todo!()
    }

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> String {
        todo!()
    }

    fn fetch_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> (Option<i64>, Option<i64>, i64, String) {
        todo!()
    }

    fn fetch_min_max(&self, query: &str, column: &str, pool: PooledConnection<PostgresConnectionManager<NoTls>>) -> (Option<i64>, Option<i64>) {
        todo!()
    }


    fn fetch_counts(&self, queries: &Vec<String>, min: Option<i64>, max: Option<i64>) -> Vec<i64> {
        todo!()
    }

    fn validate(&self) {
        // Implement extra validation here.
    }

    fn get_schema_of(&self, query: &str) -> Schema {
        todo!()
    }

    fn get_min_max_query(&self, query: &str, col: &str) -> String {
        todo!()
    }
}
