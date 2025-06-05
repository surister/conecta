use arrow::array::{ArrayBuilder, PrimitiveArray};
use arrow::datatypes::Int32Type;
use crate::schema::{NativeType, Schema};

pub trait Destination {
    fn allocate(&self, type_: NativeType, n: i64) -> Vec<Box<dyn ArrayBuilder>>;
    
    fn allocate_schema(&self, schema: Schema) -> Vec<Box<dyn ArrayBuilder>>;
}

