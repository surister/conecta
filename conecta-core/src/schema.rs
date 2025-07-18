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
    Date
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: NativeType,
    pub original_type_repr: String,
}
