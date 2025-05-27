use crate::metadata::{NeededMetadataFromSource, QueryMetadata};
use crate::source::source::{Metadata, Source};
#[derive(Debug)]
pub struct SqliteSource {
    pub conn_string: String,
}

impl Source for SqliteSource {
    fn get_conn_string(&self) -> String {
        self.conn_string.clone()
    }

    fn request_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: NeededMetadataFromSource,
    ) -> QueryMetadata {
        todo!()
    }

    fn validate(&self) {
        // Implement extra validation here.
    }
    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: NeededMetadataFromSource,
    ) -> String {
        todo!()
    }

    fn get_schema_query(&self, original_query: &str) -> String {
        todo!()
    }

    fn get_table_name(&self, query: &str) -> String {
        todo!()
    }
}
