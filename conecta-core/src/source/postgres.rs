use crate::metadata::NeededMetadataFromSource;
use crate::source::source::Source;

use r2d2_postgres::{r2d2, PostgresConnectionManager};

use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::source::schema::{Column, Schema, Type};
use tokio_postgres::NoTls;

#[derive(Debug)]
pub struct PostgresSource {
    pub conn_string: String,
}

impl Source for PostgresSource {
    fn get_conn_string(&self) -> String {
        self.conn_string.clone()
    }
    fn wrap_query_with_bounds(&self, query: &str, column: &str, bounds: (i64, i64)) -> String {
        format!(
            "select * from ({query}) where {column} >= {start:?} and {column} < {stop:?}",
            query = query,
            column = column,
            start = bounds.0,
            stop = bounds.1
        )
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
            .query(metadata_query.as_str(), &[])
            .expect("TODO: panic message");

        let row = &result[0];

        (
            row.try_get(1).ok(),
            row.try_get(2).ok(),
            row.get(0),
            metadata_query,
        )
    }
    fn validate(&self) {}

    fn get_metadata_query(
        &self,
        query: &str,
        column: Option<&str>,
        needed_metadata_from_source: &NeededMetadataFromSource,
        partition_range: Option<(i64, i64)>,
    ) -> String {
        let table_name = self.get_table_name(query);

        match needed_metadata_from_source {
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
        }
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
    fn get_schema_of(&self, query: &str) -> Schema {
        let query = self.get_schema_query(query);
        println!("{:?}", query);
        let conn = self.get_conn_string().parse().unwrap();
        let manager = PostgresConnectionManager::new(conn, NoTls);
        let pool = r2d2::Pool::builder().max_size(5).build(manager).unwrap();

        let mut client = pool.get().expect("Could not connect to the database");
        let result = client.prepare(&query);
        let columns: Vec<Column> = result.unwrap().columns().iter().map(|col| {
        Column {
            name: col.name().to_string(),
            data_type: Type::I64,
        }
    }).collect();
        println!("{:#?}", columns);
        // for i in result.unwrap() {
        //     println!("ss");
        //     println!("{:?}", i.columns())            
        // }

        Schema { columns: vec![] }
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
