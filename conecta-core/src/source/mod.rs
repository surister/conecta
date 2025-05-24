use crate::source::postgres::PostgresSource;
use crate::source::source::Source;
use crate::source::sqlite::SqliteSource;

mod postgres;
mod source;
mod sqlite;

#[derive(Debug, PartialEq)]
pub enum SourceType {
    Postgres,
    SQLite,
}

/// Parses a connection string and returns the `SourceType` depending on the uri scheme.
/// # Example:
/// ```
/// parse_conn("postgres://user:password@localhost")
/// // SourceType::Postgres
/// ```
///
/// Also supports for compatibility reasons, SQLAlchemy URIs.
/// # Example of valid URIs:
/// ```
/// "postgres+psycopg2://user:password@localhost"
/// "postgres://user:password@localhost"
/// # One database might have several valid schemes.
/// "postgresql://user:password@localhost"
/// ```
///
/// If scheme is unknown, it panics. When adding a new Database Source, it is also needed
/// to add a new Enum value.

pub fn parse_conn(conn: String) -> SourceType {
    let mut scheme = conn.split("://").next().unwrap();

    if scheme.contains("+") {
        scheme = scheme.split("+").next().unwrap();
    }

    match scheme {
        "postgres" => SourceType::Postgres,
        "postgresql" => SourceType::Postgres,
        "sqlite" => SourceType::SQLite,
        _ => {
            panic!(
                "Unknown scheme <'{}'>, do we support that database or is the \
             scheme written correctly? Tip: Correct: <'postgres'>, Incorrect: <'pstgress'>.",
                scheme
            )
        }
    }
}

pub fn get_source(source_type: SourceType) -> Box<dyn Source> {
    match source_type {
        SourceType::Postgres => Box::new(PostgresSource {}),
        SourceType::SQLite => Box::new(SqliteSource {}),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_conn_ok() {
        let result = parse_conn("postgres://user:pass@host/db_name".to_string());
        assert_eq!(SourceType::Postgres, result);

        let result = parse_conn("postgresql+psycopg2://u:p@h/d".to_string());
        assert_eq!(SourceType::Postgres, result);

        let result = parse_conn("sqlite://u:p@host/db_name".to_string());
        assert_eq!(SourceType::SQLite, result);
    }

    #[test]
    #[should_panic]
    fn parse_conn_panics() {
        parse_conn("unsuported://user:password".to_string());
    }
    #[test]
    #[should_panic]
    fn parse_conn_panic2s() {
        parse_conn("+s://user:password".to_string());
    }
}
