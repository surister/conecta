use crate::schema::{NativeType, Schema};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float16Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};

macro_rules! get_builders {
    ($schema:expr, { $($dtype:pat => $builder:ident),+ $(,)? }) => {{
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for column in &$schema.columns {
            match column.data_type {
                $(
                    $dtype => builders.push(Box::new($builder::new())),
                )+
                _ => unimplemented!("Arrow builder not implemented for {:?}", column.data_type),
            }
        }
        builders
    }};

    ($schema:expr, $capacity:expr, { $($dtype:pat => $builder:ident),+ $(,)? }) => {{
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for column in &$schema.columns {
            match column.data_type {
                $(
                    $dtype => builders.push(Box::new($builder::with_capacity($capacity))),
                )+
                _ => unimplemented!("Arrow builder not implemented for {:?}", column.data_type),
            }
        }
        builders
    }};
}

pub fn get_arrow_builders(schema: Schema) -> Vec<Box<dyn ArrayBuilder>> {
    get_builders!(schema, {
        NativeType::Bool => BooleanBuilder,

        NativeType::I8 => Int8Builder,
        NativeType::I16 => Int16Builder,
        NativeType::I32 => Int32Builder,
        NativeType::I64 => Int64Builder,

        NativeType::UI8 => UInt8Builder,
        NativeType::UI16 => UInt16Builder,
        NativeType::UI32 => UInt32Builder,
        NativeType::UI64 => UInt64Builder,

        NativeType::F16 => Float16Builder,
        NativeType::F32 => Float32Builder,
        NativeType::F64 => Float64Builder,

        NativeType::String => StringBuilder,
        NativeType::Date32 => Date32Builder
    })
}
