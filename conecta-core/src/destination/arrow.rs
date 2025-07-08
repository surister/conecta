use arrow::array::{ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, PrimitiveArray, PrimitiveBuilder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use arrow::datatypes::{Float64Type, Int32Type};
use crate::destination::destination::Destination;
use crate::schema::{NativeType, Schema};

pub struct ArrowDestination {}

pub enum MyBuilder {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
}

impl MyBuilder {

    pub fn append_nulls(&mut self, n: usize){
        match self { 
            MyBuilder::Int32(b) => b.append_nulls(n),
            MyBuilder::Float64(b) => b.append_nulls(n),
            _ => todo!()
        }
    }
}

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
    
    fn make_builders(&self, types: Vec<NativeType>, n: usize) -> Vec<MyBuilder> {
    types
        .into_iter()
        .map(|t| match t {
            NativeType::Bool => MyBuilder::Bool(BooleanBuilder::with_capacity(n)),
            NativeType::Char | NativeType::String => {
                MyBuilder::String(StringBuilder::with_capacity(n, n * 10)) // heuristic string size
            }
            NativeType::I8 => MyBuilder::Int8(Int8Builder::with_capacity(n)),
            NativeType::I16 => MyBuilder::Int16(Int16Builder::with_capacity(n)),
            NativeType::I32 => MyBuilder::Int32(Int32Builder::with_capacity(n)),
            NativeType::I64 => MyBuilder::Int64(Int64Builder::with_capacity(n)),
            NativeType::U8 => MyBuilder::UInt8(UInt8Builder::with_capacity(n)),
            NativeType::U16 => MyBuilder::UInt16(UInt16Builder::with_capacity(n)),
            NativeType::U32 => MyBuilder::UInt32(UInt32Builder::with_capacity(n)),
            NativeType::U64 => MyBuilder::UInt64(UInt64Builder::with_capacity(n)),
            NativeType::F32 => MyBuilder::Float32(Float32Builder::with_capacity(n)),
            NativeType::F64 => MyBuilder::Float64(Float64Builder::with_capacity(n)),

            NativeType::I128 | NativeType::U128 => {
                panic!("128-bit integers are not supported by Apache Arrow")
            }
        })
        .collect()
}

}