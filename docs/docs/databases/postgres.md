## Types supported

| Postgres             | Supported        | Native type              | Arrow                           | Notes |
|----------------------|------------------|--------------------------|---------------------------------|-------|
| `INT4`               | :material-check: | `i32`                    | `DataType::Int32`               |       |
| `INT8`               | :material-check: | `i64`                    | `DataType::Int64`               |       |
| `FLOAT4`             | :material-check: | `f32`                    | `DataType::Float32`             |       |
| `FLOAT8`             | :material-check: | `f64`                    | `DataType::Float64`             |       |
| `CHAR`               | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `BPCHAR`             | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `DATE`               | :material-check: | `chrono::NaiveDate`      | `DataType::Date32`              |       |
| `Array[INT]`         | :material-close: | `Vec<i32>`               | `DataType::List`                |       |
| `TEXT`               | :material-close: | `String`                 | `DataType::Utf8`                |       |
| `VARCHAR`            | :material-close: | `String`                 | `DataType::Utf8`                |       |
| `UUID`               | :material-close: | `uuid::Uuid`             | `DataType::FixedSizeBinary(16)` |       |
| `BOOL`               | :material-close: | `bool`                   | `DataType::Boolean`             |       |
| `TIMESTAMP`          | :material-close: | `chrono::NaiveDateTime`  | `DataType::Timestamp`           |       |
| `TIMESTAMPTZ`        | :material-close: | `chrono::DateTime<Utc>`  | `DataType::Timestamp`           |       |
| `BYTEA`              | :material-close: | `Vec<u8>`                | `DataType::Binary`              |       |
| `NUMERIC`            | :material-close: | `bigdecimal::BigDecimal` | `DataType::Decimal128`          |       |
| `Array[INT]`         | :material-close: | `Vec<i32>`               | `DataType::List`                |       |
| `Array[INT8]`        | :material-close: | `Vec<i64>`               | `DataType::List`                |       |
| `Array[FLOAT4]`      | :material-close: | `Vec<f32>`               | `DataType::List`                |       |
| `Array[FLOAT8]`      | :material-close: | `Vec<f64>`               | `DataType::List`                |       |
| `Array[TEXT]`        | :material-close: | `Vec<String>`            | `DataType::List`                |       |
| `Array[UUID]`        | :material-close: | `Vec<uuid::Uuid>`        | `DataType::List`                |       |
| `Array[BOOL]`        | :material-close: | `Vec<bool>`              | `DataType::List`                |       |
| `Array[DATE]`        | :material-close: | `Vec<NaiveDate>`         | `DataType::List`                |       |
| `Array[TIMESTAMP]`   | :material-close: | `Vec<NaiveDateTime>`     | `DataType::List`                |       |
| `Array[TIMESTAMPTZ]` | :material-close: | `Vec<DateTime<Utc>`      | `DataType::List`                |       |
| `Array[NUMERIC]`     | :material-close: | `Vec<BigDecimal>`        | `DataType::List`                |       |
| `Array[BYTEA]`       | :material-close: | `Vec<Vec<u8>>`           | `DataType::List`                |       |
