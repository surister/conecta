use crate::source::source::Source;
#[derive(Debug)]
pub struct SqliteSource {}

impl Source for SqliteSource {
    fn get_metadata_query(&self) -> String {
        todo!()
    }
}
