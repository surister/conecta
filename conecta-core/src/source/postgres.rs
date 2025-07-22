use arrow::array::*;
use r2d2_postgres::postgres;

use arrow::array::*;

use postgres::types::Type;
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};

use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::metadata::NeededMetadataFromSource;
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
            "select * from ({query}) where {column} >= {start:?} and {column} {last_char} {stop:?}",
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
        format!("select * from ({}) limit 0", query)
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

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> String {
        let table_name = self.get_table_name(query);

        match needed_metadata_from_source {
            NeededMetadataFromSource::MinMax => {
                let col = column.expect("Trying to use column without specifying column");
                format!(
                    "SELECT MIN({col})::bigint, \
                            MAX({col})::bigint \
                    FROM {table_name}",
                    col = col,
                    table_name = table_name
                )
            }
            NeededMetadataFromSource::CountAndMinMax => {
                let col = column.expect("Trying to use column without specifying column");
                format!(
                    "SELECT COUNT(*)::bigint, \
                        MIN({col})::bigint, \
                        MAX({col})::bigint \
                    FROM {table_name}",
                    col = col,
                    table_name = table_name
                )
            }
            NeededMetadataFromSource::Count => {
                let mut query = format!("SELECT COUNT(*)::bigint FROM ({query})");

                // If partition_range is specified by the user, we add the 'where' part.
                if let Some((min, max)) = partition_range {
                    let col = column.expect("Trying to use column without specifying column");
                    query.push_str(
                        format!(
                            " WHERE {col} > {min} and {col} < {max}",
                            col = col,
                            min = min,
                            max = max
                        )
                        .as_str(),
                    );
                }
                query
            }
            NeededMetadataFromSource::None => {
                // Fixme: Ideally we skip the metadata fetching process
                "select 1".to_string()
            }
        }
    }

    fn fetch_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> (Option<i64>, Option<i64>, i64, String) {
        let conn = self.get_conn_string().parse().unwrap();
        let manager = PostgresConnectionManager::new(conn, NoTls);

        //  todo careful here, max_size will have to be the size of the total partition.
        let pool = r2d2::Pool::builder().max_size(5).build(manager).unwrap();

        let mut client = pool.get().expect("Could not connect to the database");

        let metadata_query =
            self.get_metadata_query(query, column, needed_metadata, partition_range);

        let result = client
            .query_one(metadata_query.as_str(), &[])
            .expect("TODO: panic message");

        match needed_metadata {
            NeededMetadataFromSource::CountAndMinMax | NeededMetadataFromSource::Count => (
                result.try_get(1).ok(),
                result.try_get(2).ok(),
                result.get(0),
                metadata_query,
            ),
            NeededMetadataFromSource::MinMax => (
                result.try_get(0).ok(),
                result.try_get(1).ok(),
                0,
                metadata_query,
            ),
            // Fixme, make count option
            NeededMetadataFromSource::None => (None, None, 0, metadata_query),
        }
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
        _ => panic!("type {ty} is not implemented for Postgres"),
    }
}
