pub use crate::source::source::Source;
use std::fmt::Debug;

pub mod postgres;
mod source;

#[derive(Debug, PartialEq)]
pub enum SourceType {
    Postgres,
    MySQL,
}

/// Parses a connection string and returns the `SourceType` depending on the scheme.
/// # Example:
/// ```
/// use conecta_core::source::parse_uri;
/// parse_uri("postgres://user:password@localhost".to_string());
/// // SourceType::Postgres
/// ```
///
/// Also supports for compatibility reasons, SQLAlchemy URIs.
/// # Example of valid URIs:
/// ```python
/// "postgres+psycopg2://user:password@localhost"
/// "postgres://user:password@localhost"
/// # One database might have several valid schemes.
/// "postgresql://user:password@localhost"
/// ```
///
/// If the scheme is unknown, it panics. When adding a new Database Source, it is also needed
/// to add a new Enum value.

pub fn parse_uri(conn: String) -> SourceType {
    let mut scheme = conn.split("://").next().unwrap();

    if scheme.contains("+") {
        // sqlalchemy scheme. e.g. 'postgres+psycopg2'
        scheme = scheme.split("+").next().unwrap();
    }

    match scheme {
        "postgres" => SourceType::Postgres,
        "postgresql" => SourceType::Postgres,
        _ => panic!(
            "Unknown scheme <'{}'>, do we support that database, or is the \
                scheme written correctly? Tip: Correct: <'postgres://user:password@localhost'>,\
                Incorrect: <'pstgress://user:password@localhost'>.",
            scheme
        ),
    }
}

pub fn get_source(conn_string: &str, conn_string_type: Option<&str>) -> SourceType {
    let source_type = match conn_string_type {
        Some(conn_string_type) => {
            // The user specified a conn_string_type.
            match conn_string_type {
                "postgres" => SourceType::Postgres,
                "mysql" => SourceType::MySQL,
                _ => panic!("The specified conn_string_type is not supported."),
            }
        }
        None => {
            // User has not specified conn_string_type, we assume the format is URI.
            // https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
            parse_uri(conn_string.parse().unwrap())
        }
    };
    source_type

    // Construct the `Source` struct and validate it.
    // let source: Box<dyn Source> = match source_type {
    //     SourceType::Postgres => Box::new(PostgresSource {
    //         conn_string: conn_string.to_string(),
    //     }),
    //     SourceType::SQLite => Box::new(SqliteSource {
    //         conn_string: conn_string.to_string(),
    //     }),
    // };
    // source.validate();
    // source
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_conn_ok() {
        let result = parse_uri("postgres://user:pass@host/db_name".to_string());
        assert_eq!(SourceType::Postgres, result);

        let result = parse_uri("postgresql+psycopg2://u:p@h/d".to_string());
        assert_eq!(SourceType::Postgres, result);

        let result = parse_uri("sqlite://u:p@host/db_name".to_string());
        assert_eq!(SourceType::SQLite, result);
    }

    #[test]
    #[should_panic]
    fn parse_conn_panics() {
        parse_uri("unsuported://user:password".to_string());
    }
    #[test]
    #[should_panic]
    fn parse_conn_panic2s() {
        parse_uri("+s://user:password".to_string());
    }
}
