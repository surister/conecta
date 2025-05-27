pub(crate) use crate::metadata::Metadata;
use crate::metadata::NeededMetadataFromSource;
use crate::metadata::QueryMetadata;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fmt::Debug;
use std::println;

pub trait Source: Debug {
    /// Getter that returns the connection_string
    fn get_conn_string(&self) -> String;

    /// Method that does the necessary work and returns the metadata.
    /// Every database `Source` will have to implement their own
    fn request_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: NeededMetadataFromSource,
    ) -> QueryMetadata;

    /// Function to let database sources to implement extra validation, most databases
    /// will implement this and do nothing.
    fn validate(&self);

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: NeededMetadataFromSource,
    ) -> String;

    fn send_query(&self, query: &str) {}
    fn get_schema_query(&self, original_query: &str) -> String;
    fn get_table_name(&self, query: &str) -> String;
}
