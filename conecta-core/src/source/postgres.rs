use arrow::array::*;
use r2d2_postgres::postgres;

use arrow::array::*;

use postgres::types::Type;
use postgres::NoTls;
use r2d2_postgres::r2d2::{Pool};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::schema::{Column, NativeType, Schema};
use crate::source::source::Source;

#[derive(Debug)]
pub struct PostgresSource {
    pub conn_string: String,
}

impl Source for PostgresSource {
    fn get_conn_string(&self) -> String {
        self.conn_string.clone()
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
            "select * from ({query}) as t_inner where {column} >= {start:?} and {column} {last_char} {stop:?}",
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
        format!("select * from ({}) as t limit 0", query)
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

    fn get_min_max_query(&self, query: &str, col: &str) -> String {
        format!(
            "SELECT MIN({col})::bigint, \
                    MAX({col})::bigint \
             FROM ({query}) as t",
        )
    }

    fn fetch_min_max(
        &self,
        query: &str,
        column: &str,
        mut pool: Pool<PostgresConnectionManager<NoTls>>,
    ) -> (Option<i64>, Option<i64>) {
        let mut pool = pool.get().unwrap();
        let min_max_query = self.get_min_max_query(query, column);
        let result = pool
            .query_one(&min_max_query, &[])
            .expect("Could not fetch min/max");
        (Some(result.get(0)), Some(result.get(1)))
    }

    fn validate(&self) {}

    fn get_schema_of(&self, query: &str) -> Schema {
        let query = self.get_schema_query(query);
        let conn = self.get_conn_string().parse().unwrap();
        let manager = PostgresConnectionManager::new(conn, NoTls);
        let pool = r2d2::Pool::builder().max_size(5).build(manager).unwrap();

        let mut client = pool.get().expect("Could not connect to the database");
        let result = client.prepare(&query);
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
}

fn to_native_ty(ty: Type) -> NativeType {
    match ty {
        Type::INT4 => NativeType::I32,
        Type::INT8 => NativeType::I64,
        Type::FLOAT4 => NativeType::F32,
        Type::FLOAT8 => NativeType::F64,
        Type::CHAR | Type::TEXT => NativeType::String,
        Type::BPCHAR => NativeType::String,
        Type::DATE => NativeType::Date32,

        // Should we have a type for
        Type::TIMESTAMP => NativeType::TimestampWithoutTimeZone,
        // Type::TIMESTAMPTZ => NativeType::TimestampWithTimeZone,
        _ => panic!("type {ty} is not implemented for Postgres"),
    }
}
