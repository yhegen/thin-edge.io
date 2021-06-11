use std::str::from_utf8;

use mqtt_client::Message;

use tracing::{info, error};

#[derive(Debug)]
pub struct DvsMessage<'a> {
    pub metric_group_key: &'a str,
    pub metric_key: &'a str,
    pub metric_value: f64,
}

#[derive(thiserror::Error, Debug)]
pub enum DvsError {
    #[error(
        "Message received on invalid dvs topic: {0}. \
        Dvs message topics must be in the format  machinedata/dataupdate/<metric_group_key>/<metric-key>"
    )]
    InvalidMeasurementTopic(String),

    #[error("Invalid payload received on topic: {0}. Error: {1}")]
    InvalidMeasurementPayload(String, DvsPayloadError),
}

impl<'a> DvsMessage<'a> {
    #[cfg(test)]
    pub fn new(metric_group_key: &'a str, metric_key: &'a str, metric_value: f64) -> Self {
        Self {
            metric_group_key,
            metric_key,
            metric_value,
        }
    }

    pub fn parse_from(mqtt_message: &'a Message) -> Result<Self, DvsError> {
        let topic = mqtt_message.topic.name.as_str();
        
        let dvs_topic = match DvsTopic::from_str(topic) {
            Ok(dvs_topic) => dvs_topic,
            Err(_) => {
                return Err(DvsError::InvalidMeasurementTopic(topic.into()));
            }
        };

        let dvs_payload = DvsPayload::parse_from(mqtt_message.payload_trimmed())
            .map_err(|err| DvsError::InvalidMeasurementPayload(topic.into(), err))?;
        
        Ok(DvsMessage {
            metric_group_key: dvs_topic.metric_group_key,
            metric_key: dvs_topic.metric_key,
            metric_value: dvs_payload.metric_value,
        })
    }
}

#[derive(Debug)]
struct DvsTopic<'a> {
    metric_group_key: &'a str,
    metric_key: &'a str,
}

#[derive(Debug)]
struct InvalidDvsTopicName;

impl<'a> DvsTopic<'a> {
    fn from_str(topic_name: &'a str) -> Result<Self, InvalidDvsTopicName> {
        let mut iter = topic_name.split('/');
        let _dvs_prefix = iter.next().ok_or(InvalidDvsTopicName)?;
        let _hostname = iter.next().ok_or(InvalidDvsTopicName)?;
        let metric_group_key = iter.next().ok_or(InvalidDvsTopicName)?;
        let metric_key = iter.next().ok_or(InvalidDvsTopicName)?;

        match iter.next() {
            None => Ok(DvsTopic {
                metric_group_key,
                metric_key,
            }),
            Some(_) => Err(InvalidDvsTopicName),
        }
    }
}

#[derive(Debug)]
struct DvsPayload {
    // _timestamp: f64,
    metric_value: f64,
}

#[derive(thiserror::Error, Debug)]
pub enum DvsPayloadError {
    #[error("Non UTF-8 payload: {0:?}")]
    NonUTF8MeasurementPayload(Vec<u8>),

    #[error("Invalid payload: {0}. Expected payload format: <value>")]
    InvalidMeasurementPayloadFormat(String),

    #[error("Invalid measurement timestamp: {0}. Epoch time value expected")]
    InvalidMeasurementTimestamp(String),

    #[error("Invalid measurement value: {0}. Must be a number")]
    InvalidMeasurementValue(String),
}

impl DvsPayload {
    fn parse_from(payload: &[u8]) -> Result<Self, DvsPayloadError> {
        let payload = from_utf8(payload)
            .map_err(|_err| DvsPayloadError::NonUTF8MeasurementPayload(payload.into()))?;
        let mut iter = payload.split(':');

        // let _timestamp = iter.next().ok_or_else(|| {
        //     DvsPayloadError::InvalidMeasurementPayloadFormat(payload.to_string())
        // })?;

        // let _timestamp = _timestamp.parse::<f64>().map_err(|_err| {
        //     DvsPayloadError::InvalidMeasurementTimestamp(_timestamp.to_string())
        // })?;

        let metric_value = iter.next().ok_or_else(|| {
            DvsPayloadError::InvalidMeasurementPayloadFormat(payload.to_string())
        })?;

        let metric_value = metric_value.parse::<f64>().map_err(|_err| {
            DvsPayloadError::InvalidMeasurementValue(metric_value.to_string())
        })?;

        match iter.next() {
            None => Ok(DvsPayload {
                // _timestamp,
                metric_value,
            }),
            Some(_) => Err(DvsPayloadError::InvalidMeasurementPayloadFormat(
                payload.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use mqtt_client::Topic;

    use super::*;

    #[test]
    fn dvs_message_parsing() {
        let topic = Topic::new("dvs/localhost/temperature/value").unwrap();
        let mqtt_message = Message::new(&topic, "123456789:32.5");

        let dvs_message = DvsMessage::parse_from(&mqtt_message).unwrap();

        let DvsMessage {
            metric_group_key,
            metric_key,
            metric_value,
        } = dvs_message;

        assert_eq!(metric_group_key, "temperature");
        assert_eq!(metric_key, "value");
        assert_eq!(metric_value, 32.5);
    }

    #[test]
    fn dvs_null_terminated_message_parsing() {
        let topic = Topic::new("dvs/localhost/temperature/value").unwrap();
        let mqtt_message = Message::new(&topic, "123456789:32.5\u{0}");

        let dvs_message = DvsMessage::parse_from(&mqtt_message).unwrap();

        let DvsMessage {
            metric_group_key,
            metric_key,
            metric_value,
        } = dvs_message;

        assert_eq!(metric_group_key, "temperature");
        assert_eq!(metric_key, "value");
        assert_eq!(metric_value, 32.5);
    }

    #[test]
    fn invalid_dvs_message_topic() {
        let topic = Topic::new("dvs/less/level").unwrap();
        let mqtt_message = Message::new(&topic, "123456789:32.5");

        let result = DvsMessage::parse_from(&mqtt_message);

        assert_matches!(result, Err(DvsError::InvalidMeasurementTopic(_)));
    }

    #[test]
    fn invalid_dvs_message_payload() {
        let topic = Topic::new("dvs/host/group/key").unwrap();
        let invalid_dvs_message = Message::new(&topic, "123456789");

        let result = DvsMessage::parse_from(&invalid_dvs_message);

        assert_matches!(result, Err(DvsError::InvalidMeasurementPayload(_, _)));
    }

    #[test]
    fn invalid_dvs_topic_less_levels() {
        let result = DvsTopic::from_str("dvs/less/levels");

        assert_matches!(result, Err(InvalidDvsTopicName));
    }

    #[test]
    fn invalid_dvs_topic_more_levels() {
        let result = DvsTopic::from_str("dvs/more/levels/than/needed");

        assert_matches!(result, Err(InvalidDvsTopicName));
    }

    #[test]
    fn invalid_dvs_payload_no_seperator() {
        let payload = b"123456789";
        let result = DvsPayload::parse_from(payload);

        assert_matches!(
            result,
            Err(DvsPayloadError::InvalidMeasurementPayloadFormat(_))
        );
    }

    #[test]
    fn invalid_dvs_payload_more_seperators() {
        let payload = b"123456789:98.6:abc";
        let result = DvsPayload::parse_from(payload);

        assert_matches!(
            result,
            Err(DvsPayloadError::InvalidMeasurementPayloadFormat(_))
        );
    }

    #[test]
    fn invalid_dvs_metric_value() {
        let payload = b"123456789:abc";
        let result = DvsPayload::parse_from(payload);

        assert_matches!(
            result,
            Err(DvsPayloadError::InvalidMeasurementValue(_))
        );
    }

    #[test]
    fn invalid_dvs_metric_timestamp() {
        let payload = b"abc:98.6";
        let result = DvsPayload::parse_from(payload);

        assert_matches!(
            result,
            Err(DvsPayloadError::InvalidMeasurementTimestamp(_))
        );
    }

    #[test]
    fn very_large_metric_value() {
        let payload: Vec<u8> = format!("123456789:{}", u128::MAX).into();
        let dvs_payload = DvsPayload::parse_from(&payload).unwrap();

        assert_eq!(dvs_payload.metric_value, u128::MAX as f64);
    }

    #[test]
    fn very_small_metric_value() {
        let payload: Vec<u8> = format!("123456789:{}", i128::MIN).into();
        let dvs_payload = DvsPayload::parse_from(&payload).unwrap();

        assert_eq!(dvs_payload.metric_value, i128::MIN as f64);
    }
}
