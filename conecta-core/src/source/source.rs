use std::fmt::Debug;

pub trait Source: Debug {
    fn get_metadata_query(&self) -> String {
        "SELECT 1".to_string()
    }
}
