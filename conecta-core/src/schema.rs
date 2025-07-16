#[derive(Debug)]
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
    U8,
    U16,
    U32,
    U64,
    U128,

    F32,
    F64,

    // String types
    String,
}

#[derive(Debug)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: NativeType,
    pub original_type_repr: String
}
