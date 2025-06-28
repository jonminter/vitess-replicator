use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr,
    string::FromUtf8Error,
};

use serde_json::Number;

use crate::{
    vitess_grpc::query::{Field, Row, Type},
    vitess_schema::{FieldName, TableName, VitessSchema},
};

#[derive(Debug)]
#[non_exhaustive]
pub struct DeserializeRowError {
    pub table: Box<TableName>,
    pub column_number: usize,
    pub column_name: Box<FieldName>,
    pub kind: DeserializeRowErrorKind,
}

impl Display for DeserializeRowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error deserializing row for table `{}`", self.table)
    }
}

impl Error for DeserializeRowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            DeserializeRowErrorKind::StringFromBytesFailed(e) => Some(e),
            DeserializeRowErrorKind::SerdeJsonParseFailed(e) => Some(e),
            DeserializeRowErrorKind::UnimplementedConversion(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum DeserializeRowErrorKind {
    StringFromBytesFailed(FromUtf8Error),
    SerdeJsonParseFailed(serde_json::Error),
    UnimplementedConversion(UnimplementedConversionError),
}

#[derive(Debug)]
#[non_exhaustive]
pub struct UnimplementedConversionError {
    pub column_type: Type,
}

impl Display for UnimplementedConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "json conversion not implemented for mysql type `{:?}`",
            self.column_type
        )
    }
}

impl Error for UnimplementedConversionError {}

pub(crate) fn deserialize_row_values(
    row: Row,
    schema: &VitessSchema,
) -> Result<Vec<Option<String>>, DeserializeRowError> {
    let mut pos = 0;
    let mut row_values: Vec<Option<String>> = vec![];
    for (col_number, (data_len, (col_name, _))) in
        row.lengths.iter().zip(schema.schema.iter()).enumerate()
    {
        if *data_len < 0 {
            // Null values have -1 for length
            row_values.push(None);
            continue;
        }

        let data_len_usize: usize = data_len
            .clone()
            .try_into()
            .expect("Should be able to convert column data size from u64 to usize");

        let str_val = String::from_utf8(
            row.values.as_slice()[pos..pos + data_len_usize]
                .iter()
                .cloned()
                .collect(),
        )
        .map_err(|e| DeserializeRowError {
            table: Box::new(schema.table.clone()),
            column_number: col_number,
            column_name: Box::new(col_name.clone()),
            kind: DeserializeRowErrorKind::StringFromBytesFailed(e),
        })?;
        row_values.push(Some(str_val));
        pos += data_len_usize;
    }

    Ok(row_values)
}

fn serde_json_error(
    table_name: &TableName,
    col_number: usize,
    field_name: &FieldName,
    e: serde_json::Error,
) -> DeserializeRowError {
    DeserializeRowError {
        table: Box::new(table_name.clone()),
        column_number: col_number,
        column_name: Box::new(field_name.clone()),
        kind: DeserializeRowErrorKind::SerdeJsonParseFailed(e),
    }
}

pub(crate) fn transform_string_to_json_value(
    maybe_value: Option<String>,
    table_name: &TableName,
    column_number: usize,
    field_name: &FieldName,
    field: &Field,
) -> Result<serde_json::Value, DeserializeRowError> {
    Ok(if let Some(value) = maybe_value {
        match field.r#type() {
            Type::Varchar | Type::Char => serde_json::Value::String(value),
            Type::Varbinary | Type::Binary | Type::Blob => serde_json::Value::String(value),
            Type::Date | Type::Datetime => serde_json::Value::String(value),
            Type::Decimal
            | Type::Int8
            | Type::Int16
            | Type::Int24
            | Type::Int32
            | Type::Int64
            | Type::Float32
            | Type::Float64 => {
                let n = Number::from_str(&value)
                    .map_err(|e| serde_json_error(table_name, column_number, field_name, e))?;
                serde_json::Value::Number(n)
            }
            _ => {
                return Err(DeserializeRowError {
                    table: Box::new(table_name.clone()),
                    column_number,
                    column_name: Box::new(field_name.clone()),
                    kind: DeserializeRowErrorKind::UnimplementedConversion(
                        UnimplementedConversionError {
                            column_type: field.r#type(),
                        },
                    ),
                });
            }
        }
    } else {
        serde_json::Value::Null
    })
}
