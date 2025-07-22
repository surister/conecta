use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};


#[derive(Debug, Clone)]
pub enum NativeType {
    // Primitive types
    Bool,
    Char,

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
    
    // Time
    Date32,
    Date64
}

impl NativeType {
    fn to_arrow(self) -> DataType {
        match self {
            NativeType::I32 => DataType::Int32,
            NativeType::F64 => DataType::Float64,
            NativeType::String => DataType::Utf8,
            NativeType::Date32 => DataType::Date32,
            _ => {
                panic!("Native type to arrow not implemented. NativeType {:?}", self)
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
            self
                .columns
                .into_iter()
                .map(|column| Field::new(column.name, column.data_type.to_arrow(), true))
                .collect::<Vec<_>>()
        )
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: NativeType,
    pub original_type_repr: String,
}
