
pub trait Source {
    fn get_name(&self) -> String;
    fn get_metadata_query(&self) -> String {
        "SELECT 1".to_string()
    }
}
