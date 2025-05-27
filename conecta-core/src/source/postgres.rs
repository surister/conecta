use crate::metadata::NeededMetadataFromSource;
use crate::metadata::QueryMetadata;
use crate::source::source::{Metadata, Source};
use log::warn;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use std::fmt::format;
use std::thread;
use tokio_postgres::types::IsNull::No;
use tokio_postgres::NoTls;

#[derive(Debug)]
pub struct PostgresSource {
    pub conn_string: String,
}

impl Source for PostgresSource {
    fn get_conn_string(&self) -> String {
        self.conn_string.clone()
    }

    fn request_metadata(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata: NeededMetadataFromSource,
    ) -> QueryMetadata {
        let conn = self.get_conn_string().parse().unwrap();
        let manager = PostgresConnectionManager::new(conn, NoTls);
        let pool = r2d2::Pool::builder().max_size(5).build(manager).unwrap();

        let mut client = pool.get().unwrap();

        let query = self.get_metadata_query(&query, column, needed_metadata);
        let result = client
            .query(query.as_str(), &[])
            .expect("TODO: panic message");

        let row = &result[0];

        QueryMetadata {
            query,
            count: row.get(0),
            min_value: row.try_get(1).unwrap_or(None),
            max_value: row.try_get(2).unwrap_or(None),
        }
    }

    fn validate(&self) {}

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: NeededMetadataFromSource,
    ) -> String {
        match needed_metadata_from_source {
            NeededMetadataFromSource::CountAndMinMax => {
                let table_name = self.get_table_name(query);
                let col =
                    column.expect("Trying to get min and max metadata without specifying a column");
                format!(
                    "SELECT COUNT(*)::bigint, \
                        MIN({col})::bigint, \
                        MAX({col})::bigint \
                FROM {table_name}",
                    col = col,
                    table_name = table_name
                )
            }
            NeededMetadataFromSource::Count => format!("SELECT COUNT(*)::bigint FROM ({query})"),
        }
    }

    fn get_schema_query(&self, original_query: &str) -> String {
        format!("select * from ({}) limit 0", original_query)
    }

    fn get_table_name(&self, query: &str) -> String {
        let dialect = PostgreSqlDialect {}; // or use the dialect of your DB (e.g., MySqlDialect)
        let statements = Parser::parse_sql(&dialect, query).expect("Failed to parse SQL");

        for stmt in statements {
            if let Statement::Query(query) = stmt {
                let select = query.body.as_ref();

                if let sqlparser::ast::SetExpr::Select(select) = select {
                    let from = &select.from;

                    for table_with_joins in from {
                        let relation = &table_with_joins.relation;

                        if let TableFactor::Table { name, .. } = relation {
                            // name is an ObjectName, use to_string() or access parts
                            return name.to_string();
                        }
                    }
                }
            }
        }
        panic!("Could not extract table_name")
    }
}
