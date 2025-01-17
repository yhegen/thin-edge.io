use crate::measurement::GroupedMeasurementVisitor;
use chrono::offset::FixedOffset;
use chrono::DateTime;
use json_writer::{JsonWriter, JsonWriterError};

pub struct ThinEdgeJsonSerializer {
    json: JsonWriter,
    is_within_group: bool,
    needs_separator: bool,
    default_timestamp: Option<DateTime<FixedOffset>>,
    timestamp_present: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum ThinEdgeJsonSerializationError {
    #[error(transparent)]
    FormatError(#[from] std::fmt::Error),

    #[error(transparent)]
    MeasurementCollectorError(#[from] MeasurementStreamError),

    #[error("Serializer produced invalid Utf8 string")]
    InvalidUtf8ConversionToString(std::string::FromUtf8Error),

    #[error(transparent)]
    JsonWriterError(#[from] JsonWriterError),
}

#[derive(thiserror::Error, Debug)]
pub enum MeasurementStreamError {
    #[error("Unexpected time stamp within a group")]
    UnexpectedTimestamp,

    #[error("Unexpected end of data")]
    UnexpectedEndOfData,

    #[error("Unexpected end of group")]
    UnexpectedEndOfGroup,

    #[error("Unexpected start of group")]
    UnexpectedStartOfGroup,
}

impl ThinEdgeJsonSerializer {
    pub fn new() -> Self {
        Self::new_with_timestamp(None)
    }

    pub fn new_with_timestamp(default_timestamp: Option<DateTime<FixedOffset>>) -> Self {
        let capa = 1024; // XXX: Choose a capacity based on expected JSON length.
        let mut json = JsonWriter::with_capacity(capa);
        json.write_open_obj();

        Self {
            json,
            is_within_group: false,
            needs_separator: false,
            default_timestamp,
            timestamp_present: false,
        }
    }

    fn end(&mut self) -> Result<(), ThinEdgeJsonSerializationError> {
        if self.is_within_group {
            return Err(MeasurementStreamError::UnexpectedEndOfData.into());
        }

        if !self.timestamp_present {
            if let Some(default_timestamp) = self.default_timestamp {
                let () = self.timestamp(default_timestamp)?;
            }
        }

        self.json.write_close_obj();
        Ok(())
    }

    pub fn bytes(mut self) -> Result<Vec<u8>, ThinEdgeJsonSerializationError> {
        Ok(self.into_string()?.into_bytes())
    }

    pub fn into_string(&mut self) -> Result<String, ThinEdgeJsonSerializationError> {
        self.end()?;
        Ok(self.json.clone().into_string()?)
    }
}

impl Default for ThinEdgeJsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupedMeasurementVisitor for ThinEdgeJsonSerializer {
    type Error = ThinEdgeJsonSerializationError;

    fn timestamp(&mut self, timestamp: DateTime<FixedOffset>) -> Result<(), Self::Error> {
        if self.is_within_group {
            return Err(MeasurementStreamError::UnexpectedTimestamp.into());
        }

        if self.needs_separator {
            self.json.write_separator();
        }

        self.json.write_key("time")?;
        self.json.write_str(timestamp.to_rfc3339().as_str())?;
        self.needs_separator = true;
        self.timestamp_present = true;
        Ok(())
    }

    fn measurement(&mut self, name: &str, value: f64) -> Result<(), Self::Error> {
        if self.needs_separator {
            self.json.write_separator();
        }
        self.json.write_key(name)?;
        self.json.write_f64(value)?;
        self.needs_separator = true;
        Ok(())
    }

    fn start_group(&mut self, group: &str) -> Result<(), Self::Error> {
        if self.is_within_group {
            return Err(MeasurementStreamError::UnexpectedStartOfGroup.into());
        }

        if self.needs_separator {
            self.json.write_separator();
        }
        self.json.write_key(group)?;
        self.json.write_open_obj();
        self.needs_separator = false;
        self.is_within_group = true;
        Ok(())
    }

    fn end_group(&mut self) -> Result<(), Self::Error> {
        if !self.is_within_group {
            return Err(MeasurementStreamError::UnexpectedEndOfGroup.into());
        }

        self.json.write_close_obj();
        self.needs_separator = true;
        self.is_within_group = false;
        Ok(())
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use chrono::{offset::FixedOffset, DateTime, Local};
    fn test_timestamp() -> DateTime<FixedOffset> {
        let local_time_now: DateTime<Local> = Local::now();
        local_time_now.with_timezone(local_time_now.offset())
    }

    #[test]
    fn serialize_single_value_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        let timestamp = test_timestamp();

        serializer.timestamp(timestamp)?;
        serializer.measurement("temperature", 25.5)?;

        let body = r#""temperature":25.5"#;
        let expected_output = format!(r#"{{"time":"{}",{}}}"#, timestamp.to_rfc3339(), body);
        let output = serializer.into_string()?;
        assert_eq!(output, expected_output);
        Ok(())
    }

    #[test]
    fn serialize_single_value_no_timestamp_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        serializer.measurement("temperature", 25.5)?;
        let expected_output = r#"{"temperature":25.5}"#;
        let output = serializer.into_string()?;
        assert_eq!(output, expected_output);
        Ok(())
    }

    #[test]
    fn serialize_multi_value_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        let timestamp = test_timestamp();
        serializer.timestamp(timestamp)?;
        serializer.measurement("temperature", 25.5)?;
        serializer.start_group("location")?;
        serializer.measurement("alti", 2100.4)?;
        serializer.measurement("longi", 2200.4)?;
        serializer.measurement("lati", 2300.4)?;
        serializer.end_group()?;
        serializer.measurement("pressure", 255.0)?;
        let body = r#""temperature":25.5,"location":{"alti":2100.4,"longi":2200.4,"lati":2300.4},"pressure":255.0}"#;
        let expected_output = format!(r#"{{"time":"{}",{}"#, timestamp.to_rfc3339(), body);
        let output = serializer.into_string()?;
        assert_eq!(expected_output, output);
        Ok(())
    }

    #[test]
    fn serialize_empty_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        let expected_output = "{}";
        let output = serializer.into_string()?;
        assert_eq!(expected_output, output);
        Ok(())
    }

    #[test]
    fn serialize_timestamp_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        let timestamp = test_timestamp();
        serializer.timestamp(timestamp)?;
        let expected_output = format!(r#"{{"time":"{}"{}"#, timestamp.to_rfc3339(), "}");
        let output = serializer.into_string()?;
        assert_eq!(expected_output, output);
        Ok(())
    }

    #[test]
    fn serialize_timestamp_within_group() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        let timestamp = test_timestamp();
        serializer.start_group("location")?;
        let result = serializer.timestamp(timestamp);
        let expected_error = "Unexpected time stamp within a group";
        assert_eq!(expected_error, result.unwrap_err().to_string());
        Ok(())
    }

    #[test]
    fn serialize_unexpected_end_of_group() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        serializer.measurement("alti", 2100.4)?;
        serializer.measurement("longi", 2200.4)?;
        let result = serializer.end_group();
        let expected_error = "Unexpected end of group";
        assert_eq!(expected_error, result.unwrap_err().to_string());
        Ok(())
    }

    #[test]
    fn serialize_unexpected_start_of_group() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        serializer.start_group("location")?;
        serializer.measurement("alti", 2100.4)?;
        serializer.measurement("longi", 2200.4)?;
        let result = serializer.start_group("location");
        let expected_error = "Unexpected start of group";
        assert_eq!(expected_error, result.unwrap_err().to_string());
        Ok(())
    }

    #[test]
    fn serialize_unexpected_end_of_message() -> anyhow::Result<()> {
        let mut serializer = ThinEdgeJsonSerializer::new();
        serializer.start_group("location")?;
        serializer.measurement("alti", 2100.4)?;
        serializer.measurement("longi", 2200.4)?;
        let expected_error = "Unexpected end of data";
        let result = serializer.into_string();
        assert_eq!(expected_error, result.unwrap_err().to_string());
        Ok(())
    }
}
