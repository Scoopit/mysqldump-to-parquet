use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, SchemaBuilder, TimeUnit};
use color_eyre::eyre::{bail, Context, OptionExt, Result};
use sqlparser::{
    ast::{Expr, SetExpr},
    dialect::MySqlDialect,
    parser::Parser,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Line {
    CreateTable(String, Schema),
    /// must be in the save order as the schema
    InsertInto(String, Vec<Vec<ColumnValue>>),
    /// no operation line: anything else!
    NOP,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema(pub Vec<(String, ColumnType)>);

impl Schema {
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let mut builder = SchemaBuilder::new();
        for (column_name, column_type) in &self.0 {
            // TODO propagate the "NOT NULL" here!
            builder.push(Field::new(
                column_name.clone(),
                match column_type {
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Integer => DataType::Int64,
                    ColumnType::Float => DataType::Float64,
                    ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Second, None),
                    ColumnType::Boolean => todo!(),
                },
                true,
            ));
        }
        builder.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    /// VARCHAR, TEXT, LONGTEXT, MEDIUMTEXT
    String,
    /// INTEGER, BIGINT
    Integer,
    Float,
    /// DATE, DATETIME, TIMESTAMP
    Timestamp,
    /// BOOLEAN
    Boolean,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    String(String),
    /// INTEGER, BIGINT
    Integer(i64),
    Float(f64),
    /// BOOLEAN
    Boolean(bool),
    Null,
}
pub fn parse_line(line: &str) -> Result<Line> {
    let dialect = MySqlDialect {};
    //println!("{line}");
    let ast = Parser::parse_sql(&dialect, line)
        .with_context(|| format!("Unable to parse line: {line}"))?;

    match ast.len() {
        0 => Ok(Line::NOP),
        1 => {
            let stmt = &ast[0];
            match stmt {
                sqlparser::ast::Statement::CreateTable {
                    or_replace,
                    temporary,
                    external,
                    global,
                    if_not_exists,
                    transient,
                    name,
                    columns,
                    constraints,
                    hive_distribution,
                    hive_formats,
                    table_properties,
                    with_options,
                    file_format,
                    location,
                    query,
                    without_rowid,
                    like,
                    clone,
                    engine,
                    comment,
                    auto_increment_offset,
                    default_charset,
                    collation,
                    on_commit,
                    on_cluster,
                    order_by,
                    strict,
                } => {
                    let table_name = name.0[0].value.clone();
                    let mut schema = Vec::new();
                    for column in columns {
                        let column_name = column.name.value.clone();
                        let column_type = match column.data_type {
                            sqlparser::ast::DataType::Varchar(_) => ColumnType::String,
                            sqlparser::ast::DataType::Numeric(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Decimal(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::BigNumeric(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::BigDecimal(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Dec(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Float(_) => todo!(),
                            // should we treat tinyint(1) as boolean?
                            sqlparser::ast::DataType::TinyInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedTinyInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Int2(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedInt2(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::SmallInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedSmallInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::MediumInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedMediumInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Int(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Int4(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Int64 => ColumnType::Integer,
                            sqlparser::ast::DataType::Integer(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedInt4(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedInteger(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::BigInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedBigInt(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Int8(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::UnsignedInt8(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Float4 => ColumnType::Float,
                            sqlparser::ast::DataType::Float64 => ColumnType::Float,
                            sqlparser::ast::DataType::Real => ColumnType::Float,
                            sqlparser::ast::DataType::Float8 => ColumnType::Float,
                            sqlparser::ast::DataType::Double => ColumnType::Float,
                            sqlparser::ast::DataType::DoublePrecision => ColumnType::Float,
                            sqlparser::ast::DataType::Bool => ColumnType::Boolean,
                            sqlparser::ast::DataType::Boolean => ColumnType::Boolean,
                            sqlparser::ast::DataType::Date => ColumnType::Timestamp,
                            sqlparser::ast::DataType::Time(_, _) => ColumnType::Timestamp,
                            sqlparser::ast::DataType::Datetime(_) => ColumnType::Timestamp,
                            sqlparser::ast::DataType::Timestamp(_, _) => ColumnType::Timestamp,
                            sqlparser::ast::DataType::Text => ColumnType::String,
                            sqlparser::ast::DataType::String(_) => ColumnType::String,
                            sqlparser::ast::DataType::Enum(_) => ColumnType::String,
                            _ => bail!("Unsupported data type {:?}", column.data_type),
                        };
                        schema.push((column_name, column_type));
                    }

                    Ok(Line::CreateTable(table_name, Schema(schema)))
                }
                sqlparser::ast::Statement::Insert {
                    or,
                    ignore,
                    into,
                    table_name,
                    columns,
                    overwrite,
                    source,
                    partitioned,
                    after_columns,
                    table,
                    on,
                    returning,
                } => {
                    let table_name = table_name
                        .0
                        .get(0)
                        .ok_or_eyre("Unable to get table name from INSERT INTO statement!")?
                        .value
                        .clone();
                    let source = source.as_ref().ok_or_eyre(
                        "We are expecting a INSERT INTO ... VALUES (...) kind of statement",
                    )?;
                    if let SetExpr::Values(values) = source.body.as_ref() {
                        let mut rows = Vec::new();
                        for values in &values.rows {
                            let mut row_values = Vec::new();
                            for value in values {
                                if let Expr::Value(value) = value {
                                    let value = match value {
                                        sqlparser::ast::Value::Number(num, _) => {
                                            if num.contains(".") {
                                                ColumnValue::Float(num.parse()?)
                                            } else {
                                                ColumnValue::Integer(num.parse()?)
                                            }
                                        }
                                        sqlparser::ast::Value::SingleQuotedString(s) => {
                                            ColumnValue::String(s.clone())
                                        }
                                        sqlparser::ast::Value::Boolean(b) => {
                                            ColumnValue::Boolean(*b)
                                        }
                                        sqlparser::ast::Value::Null => ColumnValue::Null,
                                        _ => bail!("Unsupported syntax for value {value:?}"),
                                    };
                                    row_values.push(value);
                                } else {
                                    bail!("Unsupported value {value:?}");
                                }
                            }
                            rows.push(row_values);
                        }
                        Ok(Line::InsertInto(table_name, rows))
                    } else {
                        bail!("No VALUES in INSERT INTO statement!");
                    }
                }

                _ => Ok(Line::NOP),
            }
        }
        _ => color_eyre::eyre::bail!("Too much statement in line {line}"),
    }
}

#[cfg(test)]
mod test {
    use crate::line_parser::{ColumnType, ColumnValue};

    use super::{parse_line, Line};
    #[test]
    fn parse_insert_into() {
        let stmt="INSERT INTO `user` VALUES (1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', 0);";
        let line = parse_line(stmt).unwrap();
        if let Line::InsertInto(table_name, columns_values) = line {
            assert_eq!("user", table_name);
            assert_eq!(
                columns_values,
                vec![
                    vec![
                        ColumnValue::Integer(1),
                        ColumnValue::String("foobar".into()),
                        ColumnValue::Null,
                        ColumnValue::String("2012-01-02 12:55:22".into()),
                        ColumnValue::Integer(0)
                    ],
                    vec![
                        ColumnValue::Integer(1),
                        ColumnValue::String("foobar".into()),
                        ColumnValue::Null,
                        ColumnValue::String("2012-01-02 12:55:22".into()),
                        ColumnValue::Integer(0)
                    ],
                    vec![
                        ColumnValue::Integer(1),
                        ColumnValue::String("foobar".into()),
                        ColumnValue::Null,
                        ColumnValue::String("2012-01-02 12:55:22".into()),
                        ColumnValue::Integer(0)
                    ],
                    vec![
                        ColumnValue::Integer(1),
                        ColumnValue::String("foobar".into()),
                        ColumnValue::Null,
                        ColumnValue::String("2012-01-02 12:55:22".into()),
                        ColumnValue::Integer(0)
                    ]
                ]
            );
        } else {
            panic!("{line:?} is not insert into!");
        }
    }
    #[test]
    fn parse_create_table() {
        let stmt = r#"CREATE TABLE `user` (
            `id` bigint NOT NULL,
            `shortName` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
            `avatarUuid` varchar(36) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL,
            `registrationDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `premiumExpirationDate` timestamp NULL DEFAULT NULL,
            `excluded` tinyint(1) NOT NULL DEFAULT '0',
            `company_lid` bigint DEFAULT NULL,
            PRIMARY KEY (`lid`),
            UNIQUE KEY `email_index` (`email`),
            UNIQUE KEY `tel_key` (`tel`),
            KEY `authKey_index` (`authKey`),
            KEY `name_index` (`shortName`),
            KEY `registrationDate_index` (`registrationDate`),
            KEY `country_index` (`country`),
            KEY `company_lid` (`company_lid`),
            KEY `premiumExpirationDate` (`premiumExpirationDate`),
            CONSTRAINT `user_ibfk_1` FOREIGN KEY (`company_lid`) REFERENCES `company` (`lid`)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;"#;
        let line = parse_line(stmt).unwrap();
        if let Line::CreateTable(name, schema) = line {
            assert_eq!("user", name);
            assert_eq!(
                schema.0,
                vec![
                    ("id".to_string(), ColumnType::Integer),
                    ("shortName".to_string(), ColumnType::String),
                    ("avatarUuid".to_string(), ColumnType::String),
                    ("registrationDate".to_string(), ColumnType::Timestamp),
                    ("premiumExpirationDate".to_string(), ColumnType::Timestamp),
                    ("excluded".to_string(), ColumnType::Integer),
                    ("company_lid".to_string(), ColumnType::Integer),
                ]
            )
        } else {
            panic!("{line:?} is not create table!");
        }
    }
}
