use crate::destination::get_arrow_builders;
use crate::metadata::{NeededMetadataFromSource, PartitionPlan};
use crate::schema::{Column, NativeType, Schema};
use crate::source::postgres::postgres::types::WasNull;
use crate::source::source::Source;
use arrow::array::*;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use log::debug;

use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{FromSql, Type};
use postgres::{NoTls, RowIter};

use r2d2_postgres::postgres;
use r2d2_postgres::r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;

use rayon::current_thread_index;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

use crate::perf_logger::{perf_checkpoint, perf_elapsed, perf_peak_memory};
use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

/// Represents a Line, it implements FromSql to deserialize Postgres type `LINE`
/// A Line is represented by the linear equation ax + by + c = 0 where a and b are > 0.
#[derive(Debug)]
struct Line {
    a: f64,
    b: f64,
    c: f64,
}
impl FromSql<'_> for Line {
    fn from_sql<'a>(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let a = f64::from_be_bytes(raw[0..8].try_into().unwrap());
        let b = f64::from_be_bytes(raw[8..16].try_into().unwrap());
        let c = f64::from_be_bytes(raw[16..24].try_into().unwrap());

        Ok(Self { a, b, c })
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::LINE
    }
}

impl Line {
    /// Returns a vector where the components are [a, b, c] of ax + by + c = 0
    fn to_vec(&self) -> [f64; 3] {
        [self.a, self.b, self.c]
    }

    /// Same as `to_vec` but values are Option<f64>, this is just to satisfy arrow API, in reality
    /// this values will never be None.
    fn to_vec_opt(&self) -> [Option<f64>; 3] {
        [Some(self.a), Some(self.b), Some(self.c)]
    }
}

/// Represents a Circle, it implements FromSql to deserialize Postgres type `circle`
#[derive(Debug)]
struct Circle {
    /// x component of the center
    x: f64,
    /// y component of the center
    y: f64,
    /// radius length
    r: f64,
}
impl FromSql<'_> for Circle {
    fn from_sql<'a>(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let x = f64::from_be_bytes(raw[0..8].try_into().unwrap());
        let y = f64::from_be_bytes(raw[8..16].try_into().unwrap());
        let r = f64::from_be_bytes(raw[16..24].try_into().unwrap());

        Ok(Self { x, y, r })
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::CIRCLE
    }
}

impl Circle {
    /// Returns a vector where the first two components are the center's Point (x, y) and the
    /// third component is the radius length.
    fn to_vec(&self) -> [f64; 3] {
        [self.x, self.y, self.r]
    }

    /// Same as `to_vec` but values are Option<f64>, this is just to satisfy arrow API, in reality
    /// this values will never be None.
    fn to_vec_opt(&self) -> [Option<f64>; 3] {
        [Some(self.x), Some(self.y), Some(self.r)]
    }
}

#[derive(Debug)]
pub struct PostgresSource {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresSource {
    fn get_conn(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        self.pool
            .get()
            .expect("Could not generate a connection to the source database")
    }
}

macro_rules! append_column_value {
    (
        $unwrap:ident, $col_id:ident, $builder:ident, $native_type:ident,
        {
            $($type:pat => $builder_ty:ty, $value_ty:ty, $transform:expr),+ $(,)?
        }
    ) => {
        match $native_type {
            $(
                $type => {
                    let downcasted_builder = $builder
                        .as_any_mut()
                        .downcast_mut::<$builder_ty>()
                        .expect(format!("Could not downcast builder for type: {:?}", $native_type).as_str());
                    let unwrapped_value = $unwrap.try_get::<usize, $value_ty>($col_id);
                    match unwrapped_value {
                        Ok(v) => { let _ = downcasted_builder.append_value(($transform)(v));},
                        Err(e) => {
                            // If the error was WasNull, we append a null.
                            if let Some(inner) = e.into_source() {
                                if inner.downcast_ref::<WasNull>().is_some() {
                                        downcasted_builder.append_null()
                                } else {
                                    panic!("Error trying to deserialize a type, {:?}", inner)
                                }
                            }
                        },
                    }
                }
            )+
            _ => (),
        }
    };
}

impl Source for PostgresSource {
    fn process_partition_plan(
        &self,
        partition_plan: PartitionPlan,
        schema: crate::schema::Schema,
    ) -> (Vec<Vec<ArrayRef>>, crate::schema::Schema) {
        let arrays: Vec<Vec<ArrayRef>> = partition_plan
            .data_queries
            .into_par_iter()
            .map(|query| {
                let mut conn = self.get_conn();
                let count: i64;

                match partition_plan.partition_config.needed_metadata_from_source {
                    NeededMetadataFromSource::CountAndMinMax | NeededMetadataFromSource::Count
                        if partition_plan.partition_config.preallocation =>
                    {
                        let count_query = conn.query(
                            format!("SELECT count(*) FROM ({:}) as q_count", query).as_str(),
                            &[],
                        );
                        count = count_query.unwrap().get(0).unwrap().get(0);
                    }
                    _ => {
                        count = 0;
                    }
                }

                // Start data loading, using cursors (streaming until exhausted)
                let rows: RowIter = conn
                    .query_raw::<_, bool, _>(query.as_str(), vec![])
                    .expect("Query failed");

                // Create the array builders where values will be appended.
                let mut builders: Vec<Box<dyn ArrayBuilder>> =
                    get_arrow_builders(&schema, count as usize);

                debug!(
                    "thread-{}: allocated {:?}x{:?}",
                    current_thread_index().unwrap_or(0),
                    builders.len(),
                    count
                );

                let column_types: Vec<NativeType> = schema
                    .columns
                    .iter()
                    .map(|col| col.data_type.clone())
                    .collect();

                for row in rows.iterator() {
                    let unwrap = row.expect("Row is None");
                    for (col_id, builder) in builders.iter_mut().enumerate() {
                        let ty = column_types.get(col_id).expect("No column");
                        append_column_value!(unwrap, col_id, builder, ty, {
                            NativeType::I16 => Int16Builder, i16, |v | v,
                            NativeType::I32 => Int32Builder, i32, | v| v,
                            NativeType::I64 => Int64Builder, i64, | v| v,
                            NativeType::F32 => Float32Builder, f32, | v | v,
                            NativeType::F64 => Float64Builder, f64, | v | v,
                            NativeType::Bool => BooleanBuilder, bool, | v| v,
                            NativeType::Time => Time64MicrosecondBuilder, NaiveTime, |v: NaiveTime| {
                                // truncates to microseconds,
                                (v.num_seconds_from_midnight() as i64) * 1_000_000 +
                                (v.nanosecond() as i64) / 1_000
                            },
                            NativeType::Date32 => Date32Builder, NaiveDate, |v: NaiveDate|{
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("this_shouldnt_fail");
                                (v - epoch).num_days() as i32
                            },
                            NativeType::TimestampWithoutTimeZone => TimestampMicrosecondBuilder, NaiveDateTime, | v: NaiveDateTime | {
                            v.and_utc().timestamp_micros()
                        },
                            NativeType::String => StringBuilder, String, | v | v,
                            NativeType::UUID => FixedSizeBinaryBuilder, Uuid, | v | v,

                            // Vectors
                            NativeType::VecI16 => ListBuilder<Int16Builder>, Vec<Option<i16>>, | v | v,
                            NativeType::VecI32 => ListBuilder<Int32Builder>, Vec<Option<i32>>, | v | v,
                            NativeType::VecI64 => ListBuilder<Int64Builder>, Vec<Option<i64>>, | v | v,
                            NativeType::VecF32 => ListBuilder<Float32Builder>, Vec<Option<f32>>, | v | v,
                            NativeType::VecF64 => ListBuilder<Float64Builder>, Vec<Option<f64>>, | v | v,
                            NativeType::VecString => ListBuilder<StringBuilder>, Vec<Option<String>>, | v | v,
                            NativeType::VecBool => ListBuilder<BooleanBuilder>, Vec<Option<bool>>, | v | v,
                            NativeType::BidimensionalPoint => ListBuilder<Float64Builder>, geo_types::Point, |v: geo_types::Point|{
                                [Some(v.x()), Some(v.y())].into_iter()
                            },
                            NativeType::Line => ListBuilder<Float64Builder>, Line, |v: Line|{
                                v.to_vec_opt().into_iter()
                            },
                            NativeType::Circle => ListBuilder<Float64Builder>, Circle, |v: Circle|{
                                v.to_vec_opt().into_iter()
                            },
                        });

                        // VecUUID is not above because it follows a different API due to FixedSizeBinaryBuilder.
                        match ty {
                            NativeType::VecUUID => {
                                let downcasted_builder = builder
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<FixedSizeBinaryBuilder>>().expect("??");
                                let unwrapped_value = unwrap.try_get::<usize, Vec<Uuid>>(col_id);
                                match unwrapped_value {
                                    Ok(v) => {
                                        for uuid in v {
                                            let _ = downcasted_builder.values().append_value(uuid.as_bytes());
                                        }
                                        downcasted_builder.append(true);
                                    }
                                    Err(e) => {
                                        // If the error was WasNull, we append a null.
                                        if let Some(inner) = e.into_source() {
                                            if inner.downcast_ref::<WasNull>().is_some() {
                                                downcasted_builder.append_null()
                                            } else {
                                                panic!("Error trying to deserialize a type, {:?}", inner)
                                            }
                                        }
                                    }
                                }
                            },
                            _ => {}
                        }
                    }
                }

                let arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(|mut builder| builder.finish())
                    .collect::<Vec<ArrayRef>>();
                return arrays;
            })
            .collect::<Vec<_>>();

        perf_checkpoint("Finished loading data", true);

        perf_elapsed();
        perf_peak_memory();

        (arrays, schema)
    }

    // SQL creation methods.
    fn wrap_query_with_bounds(
        &self,
        query: &str,
        column: &str,
        bounds: (i64, i64),
        is_last: bool,
    ) -> String {
        let last_char = {
            if is_last {
                "<="
            } else {
                "<"
            }
        };

        format!(
            "select * from ({query}) as query_inner where {column} >= {start:?} and {column} {last_char} {stop:?}",
            query = query,
            column = column,
            start = bounds.0,
            stop = bounds.1
        )
    }

    fn merge_queries(&self, queries: &Vec<String>) -> String {
        let mut subqueries: Vec<String> = Vec::new();

        for (i, query) in queries.iter().enumerate() {
            let alias = format!("t{}", i);
            let wrapped = format!(
                "(SELECT COUNT(*) FROM ({}) AS {})",
                query.trim_end_matches(';'),
                alias
            );
            subqueries.push(wrapped);
        }

        format!("SELECT {};", subqueries.join(" +\n       "))
    }

    fn get_schema_query(&self, query: &str) -> String {
        format!("select * from ({}) as query_inner limit 0", query)
    }

    fn get_table_name(&self, query: &str) -> String {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query).expect("Failed to parse SQL");

        for stmt in statements {
            if let Statement::Query(query) = stmt {
                let select = query.body.as_ref();

                if let sqlparser::ast::SetExpr::Select(select) = select {
                    let from = &select.from;

                    for table_with_joins in from {
                        let relation = &table_with_joins.relation;

                        if let TableFactor::Table { name, .. } = relation {
                            return name.to_string();
                        }
                    }
                }
            }
        }
        panic!("Could not extract table_name")
    }

    fn fetch_min_max(&self, query: &str, column: &str) -> (Option<i64>, Option<i64>) {
        let mut pool = self.pool.get().expect("Could not get connection");
        let min_max_query = self.get_min_max_query(query, column);
        let result = pool
            .query_one(&min_max_query, &[])
            .expect("Could not fetch min/max");
        (Some(result.get(0)), Some(result.get(1)))
    }

    fn validate(&self) {}

    fn get_schema_of(&self, query: &str) -> Schema {
        let query = self.get_schema_query(query);
        let mut conn = self.get_conn();

        let result = conn.prepare(&query);
        let columns: Vec<Column> = result
            .unwrap()
            .columns()
            .iter()
            .map(|col| Column {
                name: col.name().to_string(),
                data_type: to_native_ty(col.type_().to_owned()),
                original_type_repr: col.type_().to_string(),
            })
            .collect();
        Schema { columns }
    }

    fn get_min_max_query(&self, query: &str, col: &str) -> String {
        format!(
            "SELECT MIN({col})::bigint, \
                    MAX({col})::bigint \
             FROM ({query}) as query_inner",
        )
    }
}

/// Maps a Postgres type with a `NativeType`
fn to_native_ty(ty: Type) -> NativeType {
    match ty {
        Type::INT2 => NativeType::I16,
        Type::INT4 => NativeType::I32,
        Type::INT8 => NativeType::I64,

        Type::FLOAT4 => NativeType::F32,
        Type::FLOAT8 => NativeType::F64,

        Type::CHAR | Type::BPCHAR | Type::TEXT => NativeType::String,
        Type::VARCHAR => NativeType::String,
        Type::BOOL => NativeType::Bool,
        Type::UUID => NativeType::UUID,

        // Time
        Type::DATE => NativeType::Date32,
        Type::TIMESTAMP => NativeType::TimestampWithoutTimeZone,
        Type::TIME => NativeType::Time,

        // Arrays
        Type::UUID_ARRAY => NativeType::VecUUID,
        Type::TEXT_ARRAY => NativeType::VecString,
        Type::BOOL_ARRAY => NativeType::VecBool,

        Type::INT2_ARRAY => NativeType::VecI16,
        Type::INT4_ARRAY => NativeType::VecI32,
        Type::INT8_ARRAY => NativeType::VecI64,

        Type::FLOAT4_ARRAY => NativeType::VecF32,
        Type::FLOAT8_ARRAY => NativeType::VecF64,
        Type::POINT => NativeType::BidimensionalPoint,
        Type::LINE => NativeType::Line,
        Type::CIRCLE => NativeType::Circle,

        _ => panic!("type {ty} is not implemented for Postgres"),
    }
}
