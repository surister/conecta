use arrow::array::{ArrayBuilder, Int32Builder, PrimitiveArray, PrimitiveBuilder};
use arrow::datatypes::{Float64Type, Int32Type};
use crate::destination::destination::Destination;
use crate::schema::{NativeType, Schema};

pub struct ArrowDestination {}

impl Destination for ArrowDestination {
    fn allocate(&self, type_: NativeType, n: i64) -> Vec<Box<dyn ArrayBuilder>> {
        let builder = Int32Builder::with_capacity(n as usize);
        let int_builder = Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(n as usize));
        let float_builder = Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(n as usize));

        vec![int_builder, float_builder]
        /*vec![Box::new(builder)]*/
    }

    fn allocate_schema(&self, schema: Schema) -> Vec<Box<dyn ArrayBuilder>> {
        todo!()
    }
}