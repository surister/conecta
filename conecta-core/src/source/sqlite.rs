use crate::source::source::Source;

pub struct SqliteSource {}

impl Source for SqliteSource {
    fn get_name(&self) -> String {
        "sqlite".to_string()
    }
    fn get_metadata_query(&self) -> String {
        todo!()
    }
}
