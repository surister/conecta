use std::panic;
use arrow::array::{ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, PrimitiveArray, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use arrow::datatypes::Int32Type;
use crate::destination::arrow::MyBuilder;
use crate::schema::{NativeType, Schema};

pub trait Destination {
    fn allocate(&self, type_: NativeType, n: i64) -> Vec<Box<dyn ArrayBuilder>>;
    
    fn allocate_schema(&self, schema: Schema) -> Vec<Box<dyn ArrayBuilder>>;

    fn make_builders(&self, types: Vec<NativeType>, n: usize) -> Vec<MyBuilder>;
}

