use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum NativeType {
    // Primitive types
    Bool,
    Char,
    Bytes,

    // Signed
    I8,
    I16,
    I32,
    I64,
    I128,

    // Unsigned
    UI8,
    UI16,
    UI32,
    UI64,
    UI128,

    // Float
    F16,
    F32,
    F64,

    // String types
    String,
    UUID,

    // Time
    Date32,
    Date64,
    TimestampWithoutTimeZone,
    Time,

    // Vectors
    VecBool,
    VecString,
    VecByte,
    VecUUID,
    VecChar,

    VecI8,
    VecI16,
    VecI32,
    VecI64,

    VecF16,
    VecF32,
    VecF64,

    // Geo
    BidimensionalPoint,
    Line,
    Circle,
    Box,
    LineSegment,
    Path,
    Polygon,
}

impl NativeType {
    /// Returns the `arrow` datatype equivalent.
    pub(crate) fn to_arrow(&self) -> DataType {
        match self {
            // Integers
            NativeType::I16 => DataType::Int16,
            NativeType::I32 => DataType::Int32,
            NativeType::I64 => DataType::Int64,

            // Floats
            NativeType::F16 => DataType::Float16,
            NativeType::F32 => DataType::Float32,
            NativeType::F64 => DataType::Float64,

            //Logical
            NativeType::Bool => DataType::Boolean,

            // Text
            NativeType::String => DataType::Utf8,
            NativeType::Bytes => DataType::Binary,
            NativeType::UUID => DataType::FixedSizeBinary(16),

            // Time
            NativeType::Date32 => DataType::Date32,
            NativeType::TimestampWithoutTimeZone => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            NativeType::Time => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),

            // Vectors
            NativeType::VecI16 => DataType::List(Arc::new(Field::new("_", DataType::Int16, true))),
            NativeType::VecI32 => DataType::List(Arc::new(Field::new("_", DataType::Int32, true))),
            NativeType::VecI64 => DataType::List(Arc::new(Field::new("_", DataType::Int64, true))),
            NativeType::VecF32 => {
                DataType::List(Arc::new(Field::new("_", DataType::Float32, true)))
            }
            NativeType::VecF64 => {
                DataType::List(Arc::new(Field::new("_", DataType::Float64, true)))
            }
            NativeType::BidimensionalPoint => {
                DataType::List(Arc::new(Field::new("_", DataType::Float64, true)))
            }
            NativeType::Circle => {
                DataType::List(Arc::new(Field::new("_", DataType::Float64, true)))
            }
            NativeType::Line => DataType::List(Arc::new(Field::new("_", DataType::Float64, true))),
            NativeType::LineSegment => {
                DataType::List(Arc::new(Field::new("_", DataType::Float64, true)))
            }
            NativeType::Path => DataType::List(Arc::new(Field::new("_", DataType::Float64, true))),
            NativeType::Box => DataType::List(Arc::new(Field::new("_", DataType::Float64, true))),
            NativeType::Polygon => {
                DataType::List(Arc::new(Field::new("_", DataType::Float64, true)))
            }
            NativeType::VecString => {
                DataType::List(Arc::new(Field::new("_", DataType::Utf8, true)))
            }
            NativeType::VecByte => DataType::List(Arc::new(Field::new(
                "_",
                DataType::Binary,
                true,
            ))),
            NativeType::VecUUID => DataType::List(Arc::new(Field::new(
                "_",
                DataType::FixedSizeBinary(16),
                true,
            ))),
            NativeType::VecBool => {
                DataType::List(Arc::new(Field::new("_", DataType::Boolean, true)))
            }
            _ => {
                panic!("Native type:: <{:?}> to arrow is not implemented", self)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}

// let field_a = Field::new("a", DataType::Int64, false);
impl Schema {
    pub fn to_arrow(self) -> ArrowSchema {
        ArrowSchema::new(
            self.columns
                .into_iter()
                .map(|column| Field::new(column.name, column.data_type.to_arrow(), true))
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: NativeType,
    pub original_type_repr: String,
}
