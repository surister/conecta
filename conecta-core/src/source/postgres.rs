use crate::source::source::Source;

#[derive(Debug)]
pub struct PostgresSource {}

impl Source for PostgresSource {
    fn get_metadata_query(&self) -> String {
        todo!()
    }
}
