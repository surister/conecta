use crate::metadata::NeededMetadataFromSource;
use crate::source::schema::Schema;
use std::fmt::Debug;

pub trait Source: Debug {
    /// Getter that returns the connection_string
    fn get_conn_string(&self) -> String;

    fn wrap_query_with_bounds(&self, query: &str, column: &str, bounds: (i64, i64)) -> String;

    /// Method that does the necessary work and returns the metadata.
    /// Every database `Source` will have to implement their own
    fn fetch_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> (Option<i64>, Option<i64>, i64, String);

    /// Function to let database sources to implement extra validation, most databases
    /// will implement this and do nothing.
    fn validate(&self);

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> String;

    fn get_schema_of(&self, query: &str) -> Schema {
        let query = self.get_schema_query(query);
        Schema { columns: vec![] }
    }
    fn merge_queries(&self, queries: &Vec<String>) -> String;
    fn send_query(&self, query: &str) {}
    fn get_schema_query(&self, query: &str) -> String;
    fn get_table_name(&self, query: &str) -> String;
}
