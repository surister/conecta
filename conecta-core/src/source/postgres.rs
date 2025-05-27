use crate::source::source::Source;

#[derive(Debug)]
pub struct PostgresSource {
    pub(crate) conn_string: String
}

impl Source for PostgresSource {

    fn validate(&self) {
        // Add extra validation here.
    }
    fn get_metadata_query(&self) -> String {
        todo!()
    }

    fn get_conn_string(self) -> String {
        self.conn_string
    }
}
