use crate::schema::{Schema};
use arrow::array::{make_builder, ArrayBuilder};

pub fn get_arrow_builders(schema: &Schema, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(schema.columns.len());
    for column in schema.columns.iter() {
        builders.push(
            make_builder(
                &column.data_type.to_arrow(), capacity
            )
        )
    }
   builders
}
