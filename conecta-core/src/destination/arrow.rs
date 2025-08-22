use crate::schema::{NativeType, Schema};
use arrow::array::{make_builder, ArrayBuilder, Int32Builder, ListBuilder, StringBuilder};

pub fn get_arrow_builders(schema: &Schema, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(schema.columns.len());
    for column in schema.columns.iter() {
        match column.data_type {
            NativeType::String => {
                builders.push(Box::new(StringBuilder::with_capacity(capacity, 1024)))
            }
            NativeType::VecI32 => builders.push(Box::new(ListBuilder::new(Int32Builder::new()))),
            _ => builders.push(make_builder(&column.data_type.to_arrow(), capacity)),
        }
    }
    builders
}
