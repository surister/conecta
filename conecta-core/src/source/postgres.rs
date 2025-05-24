use crate::source::source::Source;

#[derive(Debug)]
pub struct PostgresSource {}

impl Source for PostgresSource {
    fn get_name(&self) -> String {
        "postgres".to_string()
    }
    fn get_metadata_query(&self) -> String {
        todo!()
    }
}
