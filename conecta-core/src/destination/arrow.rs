use crate::schema::{NativeType, Schema};
use arrow::array::{
    make_builder, ArrayBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
};

pub fn get_arrow_builders(schema: &Schema, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(schema.columns.len());
    for column in schema.columns.iter() {
        match column.data_type {
            NativeType::String => {
                builders.push(Box::new(StringBuilder::with_capacity(capacity, 1024)))
            }
            NativeType::VecI16 => builders.push(Box::new(ListBuilder::new(Int16Builder::new()))),
            NativeType::VecI32 => builders.push(Box::new(ListBuilder::new(Int32Builder::new()))),
            NativeType::VecI64 => builders.push(Box::new(ListBuilder::new(Int64Builder::new()))),
            NativeType::VecF32 => builders.push(Box::new(ListBuilder::new(Float32Builder::new()))),
            NativeType::VecF64 => builders.push(Box::new(ListBuilder::new(Float64Builder::new()))),
            NativeType::VecUUID => {
                builders.push(Box::new(ListBuilder::new(FixedSizeBinaryBuilder::new(16))))
            }
            NativeType::BidimensionalPoint => builders.push(Box::new(ListBuilder::with_capacity(
                Float64Builder::new(),
                2,
            ))),
            _ => builders.push(make_builder(&column.data_type.to_arrow(), capacity)),
        }
    }
    builders
}
