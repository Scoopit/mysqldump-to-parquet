use arrow::datatypes::{DataType, Field, SchemaBuilder, TimeUnit};
use color_eyre::eyre::{bail, Context, OptionExt, Result};
use sqlparser::{
    ast::{Expr, SetExpr, UnaryOperator, Value},
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
pub struct Schema(pub Vec<ColumnDef>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub column_name: String,
    pub nullable: bool,
    pub column_type: ColumnType,
}

impl Schema {
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let mut builder = SchemaBuilder::new();
        for ColumnDef {
            column_name,
            nullable,
            column_type,
        } in &self.0
        {
            // TODO propagate the "NOT NULL" here!
            builder.push(Field::new(
                column_name.to_lowercase(),
                match column_type {
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Integer => DataType::Int64,
                    ColumnType::Float => DataType::Float64,
                    ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Second, None),
                    ColumnType::Boolean => todo!(),
                },
                *nullable,
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
                    or_replace: _,
                    temporary: _,
                    external: _,
                    global: _,
                    if_not_exists: _,
                    transient: _,
                    name,
                    columns,
                    constraints: _,
                    hive_distribution: _,
                    hive_formats: _,
                    table_properties: _,
                    with_options: _,
                    file_format: _,
                    location: _,
                    query: _,
                    without_rowid: _,
                    like: _,
                    clone: _,
                    engine: _,
                    comment: _,
                    auto_increment_offset: _,
                    default_charset: _,
                    collation: _,
                    on_commit: _,
                    on_cluster: _,
                    order_by: _,
                    strict: _,
                } => {
                    let table_name = name.0[0].value.clone();
                    let mut schema = Vec::new();
                    for column in columns {
                        let name = column.name.value.clone();
                        let column_type = match &column.data_type {
                            sqlparser::ast::DataType::Varchar(_) => ColumnType::String,
                            sqlparser::ast::DataType::Numeric(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Decimal(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::BigNumeric(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::BigDecimal(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Dec(_) => ColumnType::Integer,
                            sqlparser::ast::DataType::Float(_) => ColumnType::Float,
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
                            sqlparser::ast::DataType::Custom(name, _) => {
                                let type_name = name.0[0].value.as_str();
                                match type_name {
                                    "longtext" => ColumnType::String,
                                    "mediumtext" => ColumnType::String,
                                    _ => bail!("Unsupported data type {:?}", column.data_type),
                                }
                            }
                            _ => bail!("Unsupported data type {:?}", column.data_type),
                        };
                        println!("{name} {:?}", column.options);
                        schema.push(ColumnDef {
                            column_name: name,
                            nullable: column
                                .options
                                .iter()
                                .map(|column_option| match column_option.option {
                                    sqlparser::ast::ColumnOption::Null => Some(true),
                                    sqlparser::ast::ColumnOption::NotNull => Some(false),
                                    sqlparser::ast::ColumnOption::Unique { is_primary }
                                        if is_primary == true =>
                                    {
                                        Some(false)
                                    }
                                    _ => None,
                                })
                                .filter(Option::is_some)
                                .next()
                                .flatten()
                                .unwrap_or(true),
                            column_type,
                        });
                    }

                    Ok(Line::CreateTable(table_name, Schema(schema)))
                }
                sqlparser::ast::Statement::Insert {
                    or: _,
                    ignore: _,
                    into: _,
                    table_name,
                    columns: _,
                    overwrite: _,
                    source,
                    partitioned: _,
                    after_columns: _,
                    table: _,
                    on: _,
                    returning: _,
                } => {
                    let table_name = table_name
                        .0
                        .first()
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
                                match value {
                                    Expr::UnaryOp { op, expr } if *op == UnaryOperator::Minus => {
                                        // case of negative numbers...
                                        let Expr::Value(Value::Number(num, _)) = expr.as_ref()
                                        else {
                                            bail!("Unknown expr with a minus operator {expr}")
                                        };
                                        if num.contains('.') {
                                            row_values.push(ColumnValue::Float(-num.parse()?));
                                        } else {
                                            row_values.push(ColumnValue::Integer(-num.parse()?));
                                        }
                                    }
                                    Expr::Value(value) => {
                                        let value = match value {
                                            sqlparser::ast::Value::Number(num, _) => {
                                                if num.contains('.') {
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
                                    }
                                    _ => {
                                        bail!("Unsupported value {value:?}");
                                    }
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

    use crate::line_parser::{ColumnDef, ColumnType, ColumnValue};

    use super::{parse_line, Line};
    #[test]
    fn parse_insert_into() {
        let stmt="INSERT INTO `user` VALUES (1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', 0),(1, 'foobar', NULL, '2012-01-02 12:55:22', -123);";
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
                        ColumnValue::Integer(-123)
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
                    ColumnDef {
                        column_name: "id".into(),
                        nullable: false,
                        column_type: ColumnType::Integer,
                    },
                    ColumnDef {
                        column_name: "shortName".into(),
                        nullable: false,
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        column_name: "avatarUuid".into(),
                        nullable: true,
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        column_name: "registrationDate".into(),
                        nullable: false,
                        column_type: ColumnType::Timestamp,
                    },
                    ColumnDef {
                        column_name: "premiumExpirationDate".into(),
                        nullable: true,
                        column_type: ColumnType::Timestamp,
                    },
                    ColumnDef {
                        column_name: "excluded".into(),
                        nullable: false,
                        column_type: ColumnType::Integer,
                    },
                    ColumnDef {
                        column_name: "company_lid".into(),
                        nullable: true,
                        column_type: ColumnType::Integer,
                    },
                ]
            )
        } else {
            panic!("{line:?} is not create table!");
        }
    }
}
