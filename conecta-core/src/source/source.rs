use std::fmt::Debug;

pub trait Source: Debug {
    // Validates that the given information is valid.
    fn validate(&self){

    }

    fn get_metadata_query(&self) -> String {
        "SELECT 1".to_string()
    }

    fn send_query(&self, query: &str){

    }
    fn get_conn_string(self) -> String;
}
