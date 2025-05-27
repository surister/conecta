use crate::source::source::Source;
#[derive(Debug)]
pub struct SqliteSource {
    pub conn_string: String
}

impl Source for SqliteSource {
    fn validate(&self) {
        // Implement extra validation here.
    }
    fn get_metadata_query(&self) -> String {
        todo!()
    }
    fn get_conn_string(self) -> String {
        self.conn_string
    }
}
