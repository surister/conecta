use crate::metadata::NeededMetadataFromSource;
use crate::schema::Schema;
use std::fmt::Debug;

pub trait Source: Debug + Send + Sync {
    /// Getter that returns the connection_string.
    fn get_conn_string(&self) -> String;

    /// Wraps a given SQL query to only give values within the given `bounds`, on the given column.
    ///
    /// The implementation is source dependant as every database might have different syntax.
    ///
    /// # Example:
    /// ```
    /// wrap_query_with_bounds("select 1 from tbl", "mycolumn", (1, 10));
    ///  "select * from (select * from tbl1 ) as t where t.mycolumn > 1 and t.mycolumn < 10"
    /// ```
    fn wrap_query_with_bounds(&self, query: &str, column: &str, bounds: (i64, i64), is_last: bool) -> String;

    fn merge_queries(&self, queries: &Vec<String>) -> String;

    /// Returns a SQL query that returns the schema of a given query, it can either be
    /// a `LIMIT 0` query or a query to a metadata table, it's source dependant.
    fn get_schema_query(&self, query: &str) -> String;

    /// Returns the name the query's main return table.
    // TODO: Check if this work on more complex queries like CTEs
    fn get_table_name(&self, query: &str) -> String;

    /// Returns the SQL query that will be used to get metadata: min, max and/or count, depending
    /// on `needed_metadata_from_source`.
    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> String;

    /// Runs the query created from `get_metadata_query` and parses its results.
    /// It's source dependant.
    fn fetch_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> (Option<i64>, Option<i64>, i64, String);

    /// Lets database sources to implement extra validation, most sources
    /// will implement this and do nothing.
    fn validate(&self);

    fn get_schema_of(&self, query: &str) -> Schema;

    fn send_query(&self, query: &str) {}
}
