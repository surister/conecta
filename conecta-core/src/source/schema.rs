#[derive(Debug)]
pub enum Type {
    // Primitive types
    Bool,
    
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


pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: Type,
}
