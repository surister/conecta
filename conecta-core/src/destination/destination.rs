use crate::schema::{NativeType, Schema};
use arrow::array::{
    ArrayBuilder,
};

pub trait Destination {
    fn allocate(&self, type_: NativeType, n: i64) -> Vec<Box<dyn ArrayBuilder>>;

    fn allocate_schema(&self, schema: Schema) -> Vec<Box<dyn ArrayBuilder>>;
}
