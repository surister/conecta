Source for [PostgresSQL](https://www.postgresql.org/) database.

## URI

The supported URI format is:

`scheme://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`

For example:

```shell
postgresql://user:password@localhost:5432/postgres_database
```

Several schemes are supported, and others like sqlalchemy's are supported for compatability.

Valid schemes:

```shell
postgresql://
postgres://
postgres+psycopg2://
postgresql+psycopg://
```

In the case of sqlalchemy, the part on the right e.g. `psycopg2` is ignored.

## Types supported

### Primitive datatypes

| Postgres type | Supported        | Native type              | Arrow                           | Notes |
|---------------|------------------|--------------------------|---------------------------------|-------|
| `BOOL`        | :material-check: | `bool`                   | `DataType::Boolean`             |       |
| `INT2`        | :material-check: | `16`                     | `DataType::Int16`               |       |
| `INT4`        | :material-check: | `i32`                    | `DataType::Int32`               |       |
| `INT8`        | :material-check: | `i64`                    | `DataType::Int64`               |       |
| `FLOAT4`      | :material-check: | `f32`                    | `DataType::Float32`             |       |
| `FLOAT8`      | :material-check: | `f64`                    | `DataType::Float64`             |       |
| `CHAR`        | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `BPCHAR`      | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `TEXT`        | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `VARCHAR`     | :material-check: | `String`                 | `DataType::Utf8`                |       |
| `UUID`        | :material-check: | `uuid::Uuid`             | `DataType::FixedSizeBinary(16)` |       |
| `BYTEA`       | :material-close: | `Vec<u8>`                | `DataType::Binary`              |       |
| `NUMERIC`     | :material-close: | `bigdecimal::BigDecimal` | `DataType::Decimal128`          |       |

### Time datatypes

| Postgres type | Supported        | Native type             | Arrow                                        | Notes                     |
|---------------|------------------|-------------------------|----------------------------------------------|---------------------------|
| `DATE`        | :material-check: | `chrono::NaiveDate`     | `DataType::Date32`                           | 32 bit                    |
| `TIME`        | :material-check: | `chrono::NaiveDateTime` | `DataType::Time64(TimeUnit::Microsecond)`    | precision is microseconds |
| `TIMESTAMP`   | :material-check: | `chrono::NaiveDateTime` | `DataType::Timestamp<TimeUnit::Microsecond>` | precision is microseconds |
| `TIMESTAMPTZ` | :material-close: | `chrono::DateTime<Utc>` | `DataType::Timestamp`                        |                           |

### Geo-spatial datatypes

These geospatial types are the
native [ones](https://www.postgresql.org/docs/current/datatype-geometric.html)
not PostGis'.

| Postgres type | Supported        | Native type                  | Arrow            | Notes                                                                                                                                                                           |
|---------------|------------------|------------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `POINT`       | :material-check: | `geo::Point`                 | ``list<double>`` | List with two elements representing a point (x, y)                                                                                                                              |
| `CIRCLE`      | :material-check: | `conecta::postgres::Circle`  | ``list<double>`` | List with three elements representing the center (x, y) and `r` and radius. (x, y, r)                                                                                           |
| `LINE`        | :material-check: | `conecta::postgres::Line`    | ``list<double>`` | List with three elements (a, b, c) from `a`x + `b`y + `c` = 0 linear equation                                                                                                   |
| `BOX`         | :material-check: | `conecta::postgres::Box`     | ``list<double>`` | List with four elements, (x1, y1, x2, y2) where (x1, y1) and (x2, y2) are any two opposite corners of the box                                                                   |
| `LSEG`        | :material-check: | `conecta::postgres::Lseg`    | ``list<double>`` | List with four elements, (x1, y1, x2, y2) where (x1,y1) and (x2,y2) are the end points of the line segment.                                                                     |
| `PATH`        | :material-check: | `conecta::postgres::Path`    | ``list<double>`` | List with minimum of two elements, (o, c, x1, y1, x2, y2...) where `o` is whether the path is open or not, `c` is the total count of points and the rest are points components. |
| `POLYGON`     | :material-check: | `conecta::postgres::Polygon` | ``list<double>`` | List with points, (x1, y1, x2, y2... xn, yn)                                                                                                                                    |

### Array datatypes

| Postgres type        | Supported        | Native type          | Arrow                 | Notes |
|----------------------|------------------|----------------------|-----------------------|-------|
| `INT2_ARRAY`         | :material-check: | `Vec<i16>`           | `DataType::List<i16>` |       |
| `INT4_ARRAY`         | :material-check: | `Vec<i32>`           | `DataType::List<i32>` |       |
| `INT8_ARRAY`         | :material-check: | `Vec<i64>`           | `DataType::List<i64>` |       |
| `FLOAT4_ARRAY`       | :material-check: | `Vec<f32>`           | `DataType::List<f32>` |       |
| `FLOAT8_ARRAY`       | :material-check: | `Vec<f64>`           | `DataType::List<f64>` |       |
| `Array[TEXT]`        | :material-close: | `Vec<String>`        | `DataType::List`      |       |
| `Array_UUID`         | :material-check: | `Vec<uuid::Uuid>`    | `DataType::List`      |       |
| `Array[BOOL]`        | :material-check: | `Vec<bool>`          | `DataType::List`      |       |
| `Array[DATE]`        | :material-close: | `Vec<NaiveDate>`     | `DataType::List`      |       |
| `Array[TIMESTAMP]`   | :material-close: | `Vec<NaiveDateTime>` | `DataType::List`      |       |
| `Array[TIMESTAMPTZ]` | :material-close: | `Vec<DateTime<Utc>`  | `DataType::List`      |       |
| `Array[NUMERIC]`     | :material-close: | `Vec<BigDecimal>`    | `DataType::List`      |       |
| `Array[BYTEA]`       | :material-close: | `Vec<Vec<u8>>`       | `DataType::List`      |       |

## Example

```python
from conecta import read_sql

t = read_sql(
    conn="postgres://postgres:postgres@localhost:5432",
    queries=['select * from lineitem'],
    partition_on='l_orderkey',
    partition_num=4,
)
```