//! Python bindings for robocodec.
//!
//! Thin PyO3 wrappers that expose existing robocodec APIs to Python.
//! No business logic here - just type conversions and error handling.

use crate::{
    encoding::{cdr::CdrDecoder, json::JsonDecoder, protobuf::ProtobufDecoder},
    format::mcap::transform::TransformBuilder,
    reader::{DecodedMessageStream, RoboReader},
    rewriter::RewriteOptions,
    schema::parse_schema,
    ChannelInfo, CodecError, CodecValue, DecodedMessage,
};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use std::collections::HashMap;

// =============================================================================
// Error conversion
// =============================================================================

fn codec_error_to_py(error: CodecError) -> PyErr {
    let msg = error.to_string();
    // Map error types based on message content
    if msg.contains("Failed to open") || msg.contains("No such file") || msg.contains("not found") {
        PyIOError::new_err(msg)
    } else if msg.contains("Invalid") || msg.contains("parse") || msg.contains("unknown") {
        PyValueError::new_err(msg)
    } else {
        PyRuntimeError::new_err(msg)
    }
}

// =============================================================================
// CodecValue conversion
// =============================================================================

/// Convert CodecValue to Python native type.
fn codec_value_to_py(value: &CodecValue, py: Python<'_>) -> PyResult<PyObject> {
    match value {
        CodecValue::Bool(b) => Ok(PyObject::from(b.into_pyobject(py)?.to_owned())),
        CodecValue::Int8(i) => Ok(PyObject::from(i.into_pyobject(py)?.to_owned())),
        CodecValue::Int16(i) => Ok(PyObject::from(i.into_pyobject(py)?.to_owned())),
        CodecValue::Int32(i) => Ok(PyObject::from(i.into_pyobject(py)?.to_owned())),
        CodecValue::Int64(i) => Ok(PyObject::from(i.into_pyobject(py)?.to_owned())),
        CodecValue::UInt8(u) => Ok(PyObject::from(u.into_pyobject(py)?.to_owned())),
        CodecValue::UInt16(u) => Ok(PyObject::from(u.into_pyobject(py)?.to_owned())),
        CodecValue::UInt32(u) => Ok(PyObject::from(u.into_pyobject(py)?.to_owned())),
        CodecValue::UInt64(u) => Ok(PyObject::from(u.into_pyobject(py)?.to_owned())),
        CodecValue::Float32(f) => Ok(PyObject::from(f64::from(*f).into_pyobject(py)?.to_owned())),
        CodecValue::Float64(f) => Ok(PyObject::from(f.into_pyobject(py)?.to_owned())),
        CodecValue::String(s) => Ok(PyObject::from(s.into_pyobject(py)?.to_owned())),
        CodecValue::Bytes(b) => Ok(PyObject::from(b.as_slice().into_pyobject(py)?.to_owned())),
        CodecValue::Timestamp(n) => Ok(PyObject::from(n.into_pyobject(py)?.to_owned())),
        CodecValue::Duration(n) => Ok(PyObject::from(n.into_pyobject(py)?.to_owned())),
        CodecValue::Null => Ok(PyObject::from(py.None())),
        CodecValue::Array(arr) => {
            let list = PyList::empty(py);
            for v in arr {
                list.append(codec_value_to_py(v, py)?)?;
            }
            Ok(PyObject::from(list))
        }
        CodecValue::Struct(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, codec_value_to_py(value, py)?)?;
            }
            Ok(PyObject::from(dict))
        }
    }
}

/// Convert ChannelInfo to Python dict.
fn channel_info_to_py(channel: &ChannelInfo, py: Python<'_>) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("id", channel.id)?;
    dict.set_item("topic", &channel.topic)?;
    dict.set_item("message_type", &channel.message_type)?;
    dict.set_item("encoding", &channel.encoding)?;
    dict.set_item("message_count", channel.message_count)?;
    Ok(PyObject::from(dict))
}

// =============================================================================
// Encoding helpers
// =============================================================================

/// Convert Python dict to DecodedMessage.
pub(crate) fn py_dict_to_decoded_message(dict: &Bound<'_, PyDict>) -> PyResult<DecodedMessage> {
    let mut message = DecodedMessage::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        let codec_value = py_value_to_codec_value(&value)?;
        message.insert(key_str, codec_value);
    }
    Ok(message)
}

/// Convert Python value to CodecValue.
pub(crate) fn py_value_to_codec_value(value: &Bound<'_, pyo3::PyAny>) -> PyResult<CodecValue> {
    if value.is_none() {
        Ok(CodecValue::Null)
    } else if let Ok(b) = value.extract::<bool>() {
        Ok(CodecValue::Bool(b))
    } else if let Ok(i) = value.extract::<i64>() {
        Ok(CodecValue::Int64(i))
    } else if let Ok(u) = value.extract::<u64>() {
        Ok(CodecValue::UInt64(u))
    } else if let Ok(f) = value.extract::<f64>() {
        Ok(CodecValue::Float64(f))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(CodecValue::String(s))
    } else if let Ok(b) = value.extract::<Vec<u8>>() {
        Ok(CodecValue::Bytes(b))
    } else if let Ok(list) = value.downcast::<PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_value_to_codec_value(&item)?);
        }
        Ok(CodecValue::Array(arr))
    } else if let Ok(d) = value.downcast::<PyDict>() {
        let mut map = HashMap::new();
        for (key, val) in d.iter() {
            let key_str: String = key.extract()?;
            map.insert(key_str, py_value_to_codec_value(&val)?);
        }
        Ok(CodecValue::Struct(map))
    } else {
        Err(PyValueError::new_err(format!(
            "Unsupported type for CodecValue: {:?}",
            value.get_type()
        )))
    }
}

// =============================================================================
// Schema parsing
// =============================================================================

/// Parse a ROS/IDL schema string.
///
/// Args:
///     type_name: Name of the message type (e.g., "std_msgs/String")
///     schema_text: Schema definition text
///
/// Returns:
///     Schema object (for internal use, returns serialized form)
#[pyfunction]
fn parse_schema_text(type_name: &str, schema_text: &str) -> PyResult<String> {
    let _schema = parse_schema(type_name, schema_text).map_err(codec_error_to_py)?;
    // For now return a simple confirmation
    // In the future we might want to return a proper schema object
    Ok(format!("Schema parsed: {}", type_name))
}

// =============================================================================
// Decoders
// =============================================================================

/// CDR decoder for ROS1/ROS2 messages.
#[pyclass(name = "CdrDecoder")]
pub struct PyCdrDecoder {
    decoder: CdrDecoder,
}

#[pymethods]
impl PyCdrDecoder {
    /// Create a new CDR decoder.
    #[new]
    fn new() -> Self {
        Self {
            decoder: CdrDecoder::new(),
        }
    }

    /// Decode CDR-encoded binary data.
    ///
    /// Args:
    ///     schema_text: Schema definition text
    ///     type_name: Message type name
    ///     data: Binary data to decode
    ///
    /// Returns:
    ///     Dictionary with decoded fields
    fn decode(
        &self,
        py: Python<'_>,
        schema_text: &str,
        type_name: &str,
        data: &[u8],
    ) -> PyResult<PyObject> {
        let schema = parse_schema(type_name, schema_text).map_err(codec_error_to_py)?;

        let message = self
            .decoder
            .decode(&schema, data, Some(type_name))
            .map_err(codec_error_to_py)?;

        codec_value_to_py(&CodecValue::Struct(message), py)
    }
}

/// Protobuf decoder for protobuf-encoded messages.
#[pyclass(name = "ProtobufDecoder")]
pub struct PyProtobufDecoder {
    decoder: ProtobufDecoder,
}

#[pymethods]
impl PyProtobufDecoder {
    /// Create a new Protobuf decoder.
    #[new]
    fn new() -> Self {
        Self {
            decoder: ProtobufDecoder::new(),
        }
    }

    /// Decode protobuf binary data.
    ///
    /// Args:
    ///     data: Binary data to decode
    ///
    /// Returns:
    ///     Dictionary with decoded fields
    fn decode(&self, py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
        let message = self.decoder.decode(data).map_err(codec_error_to_py)?;
        codec_value_to_py(&CodecValue::Struct(message), py)
    }
}

/// JSON decoder for JSON-encoded messages.
#[pyclass(name = "JsonDecoder")]
pub struct PyJsonDecoder {
    decoder: JsonDecoder,
}

#[pymethods]
impl PyJsonDecoder {
    /// Create a new JSON decoder.
    #[new]
    fn new() -> Self {
        Self {
            decoder: JsonDecoder::new(),
        }
    }

    /// Decode JSON string.
    ///
    /// Args:
    ///     json_text: JSON string to decode
    ///
    /// Returns:
    ///     Dictionary with decoded fields
    fn decode(&self, py: Python<'_>, json_text: &str) -> PyResult<PyObject> {
        let message = self.decoder.decode(json_text).map_err(codec_error_to_py)?;
        codec_value_to_py(&CodecValue::Struct(message), py)
    }
}

// =============================================================================
// Message iterator
// =============================================================================

/// Iterator for decoded messages from a robotics data file.
#[pyclass(name = "MessageIter", unsendable)]
pub struct PyMessageIter {
    stream: Box<dyn DecodedMessageStream>,
}

#[pymethods]
impl PyMessageIter {
    /// Return the next decoded message as a tuple (message_dict, channel_info_dict).
    /// Returns None when exhausted.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match slf.stream.next() {
            Some(Ok((msg, channel))) => {
                let msg_obj = codec_value_to_py(&CodecValue::Struct(msg), py)?;
                let channel_obj = channel_info_to_py(&channel, py)?;
                let tuple = (msg_obj, channel_obj);
                Ok(Some(PyObject::from(tuple.into_pyobject(py)?.to_owned())))
            }
            Some(Err(e)) => Err(codec_error_to_py(e)),
            None => Ok(None),
        }
    }
}

// =============================================================================
// Reader - Message iteration
// =============================================================================

/// Reader for robotics data files (MCAP, ROS1 bag).
#[pyclass(name = "Reader")]
pub struct PyReader {
    reader: RoboReader,
}

#[pymethods]
impl PyReader {
    /// Open a robotics data file (auto-detects format from extension).
    ///
    /// Args:
    ///     path: Path to MCAP or BAG file
    ///
    /// Returns:
    ///     Reader instance
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let reader = RoboReader::open(path).map_err(codec_error_to_py)?;
        Ok(Self { reader })
    }

    /// Get information about all channels/topics in the file.
    ///
    /// Returns:
    ///     List of channel info dictionaries
    fn channels(&self, py: Python<'_>) -> PyResult<PyObject> {
        let channels = self.reader.channels();
        let list = PyList::empty(py);

        for (_id, channel) in channels {
            list.append(channel_info_to_py(channel, py)?)?;
        }

        Ok(PyObject::from(list))
    }

    /// Get the total number of messages in the file.
    fn message_count(&self) -> PyResult<u64> {
        Ok(self.reader.message_count())
    }

    /// Iterate over decoded messages.
    ///
    /// Returns:
    ///     MessageIter that yields tuples of (message_dict, channel_info_dict)
    fn iter_messages(&self) -> PyResult<PyMessageIter> {
        let stream = self.reader.decode_messages().map_err(codec_error_to_py)?;
        Ok(PyMessageIter { stream })
    }
}

// =============================================================================
// Convert - Format conversion with transforms
// =============================================================================

/// Convert between robotics data formats (BAG ↔ MCAP) with optional transforms.
///
/// This function uses RoboRewriter which automatically detects the input format
/// and applies transformations during conversion.
///
/// Args:
///     input_path: Input file path
///     output_path: Output file path
///     rename: Optional dict mapping old topics to new topics
///     type_rename: Optional dict mapping old types to new types
///
/// Example:
///     convert("input.bag", "output.mcap", rename={"/old": "/new"})
#[pyfunction]
fn convert(
    input_path: &str,
    output_path: &str,
    rename: Option<Bound<'_, PyDict>>,
    type_rename: Option<Bound<'_, PyDict>>,
) -> PyResult<()> {
    // Build rewrite options with transforms if provided
    let mut options = RewriteOptions::default();

    if rename.is_some() || type_rename.is_some() {
        let mut builder = TransformBuilder::new();

        if let Some(rename_dict) = rename {
            for (key, val) in rename_dict.iter() {
                let old: String = key.extract()?;
                let new: String = val.extract()?;
                builder = builder.with_topic_rename(old, new);
            }
        }

        if let Some(type_dict) = type_rename {
            for (key, val) in type_dict.iter() {
                let old: String = key.extract()?;
                let new: String = val.extract()?;
                builder = builder.with_type_rename(old, new);
            }
        }

        options = options.with_transforms(builder.build());
    }

    // Use RoboRewriter for unified conversion with transform support
    let mut rewriter = crate::rewriter::RoboRewriter::with_options(input_path, options)
        .map_err(codec_error_to_py)?;

    rewriter.rewrite(output_path).map_err(codec_error_to_py)?;

    Ok(())
}

/// Transform an existing file (alias for convert with same format).
///
/// Args:
///     input_path: Input file path
///     output_path: Output file path (None = overwrite input file)
///     rename: Optional dict mapping old topics to new topics
///     type_rename: Optional dict mapping old types to new types
#[pyfunction]
fn transform(
    input_path: &str,
    output_path: Option<&str>,
    rename: Option<Bound<'_, PyDict>>,
    type_rename: Option<Bound<'_, PyDict>>,
) -> PyResult<()> {
    let output = output_path.unwrap_or(input_path);
    convert(input_path, output, rename, type_rename)
}

/// Read messages from a robotics data file.
///
/// Args:
///     path: Path to MCAP or BAG file
///
/// Returns:
///     Reader instance
#[pyfunction]
fn read(path: &str) -> PyResult<PyReader> {
    let reader = RoboReader::open(path).map_err(codec_error_to_py)?;
    Ok(PyReader { reader })
}

/// Decode binary data with schema.
///
/// Args:
///     data: Binary data to decode
///     schema_text: Schema definition text
///     type_name: Message type name
///     encoding: Encoding format ("cdr", "protobuf", or "json")
///
/// Returns:
///     Dictionary with decoded fields
#[pyfunction]
fn decode(
    py: Python<'_>,
    data: &[u8],
    schema_text: Option<&str>,
    type_name: Option<&str>,
    encoding: &str,
) -> PyResult<PyObject> {
    match encoding {
        "cdr" => {
            if let (Some(schema), Some(type_name)) = (schema_text, type_name) {
                let decoder = CdrDecoder::new();
                let schema = parse_schema(type_name, schema).map_err(codec_error_to_py)?;
                let message = decoder
                    .decode(&schema, data, Some(type_name))
                    .map_err(codec_error_to_py)?;
                codec_value_to_py(&CodecValue::Struct(message), py)
            } else {
                Err(PyValueError::new_err(
                    "CDR decoding requires schema_text and type_name",
                ))
            }
        }
        "protobuf" => {
            let decoder = ProtobufDecoder::new();
            let message = decoder.decode(data).map_err(codec_error_to_py)?;
            codec_value_to_py(&CodecValue::Struct(message), py)
        }
        "json" => {
            let json_text = std::str::from_utf8(data)
                .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in JSON data: {}", e)))?;
            let decoder = JsonDecoder::new();
            let message = decoder.decode(json_text).map_err(codec_error_to_py)?;
            codec_value_to_py(&CodecValue::Struct(message), py)
        }
        _ => Err(PyValueError::new_err(format!(
            "Unknown encoding: {} (supported: cdr, protobuf, json)",
            encoding
        ))),
    }
}

// =============================================================================
// Encode - Convert Python dict to binary
// =============================================================================

/// Encode a Python dict to binary message.
///
/// Args:
///     message: Dictionary with message fields
///     schema_text: Schema definition text (required for CDR)
///     type_name: Message type name (e.g., "std_msgs/String")
///     encoding: Encoding format ("cdr", "protobuf", or "json")
///
/// Returns:
///     Tuple of (encoded_bytes, metadata_dict) where metadata contains:
///     - "encoding": The encoding format used
///     - "type_name": The message type name
///     - "length": Number of bytes in encoded data
///
/// Example:
///     >>> data, meta = robocodec.encode(
///     ...     {"data": "hello"},
///     ...     schema_text="string data",
///     ...     type_name="std_msgs/String",
///     ...     encoding="cdr"
///     ... )
///     >>> print(f"Encoded {len(data)} bytes")
///     Encoded 13 bytes
#[pyfunction]
fn encode(
    py: Python<'_>,
    message: &Bound<'_, PyDict>,
    schema_text: Option<&str>,
    type_name: Option<&str>,
    encoding: &str,
) -> PyResult<(PyObject, PyObject)> {
    let decoded = py_dict_to_decoded_message(message)?;

    let (data, encoding_name) = match encoding {
        "cdr" => {
            let schema = schema_text
                .ok_or_else(|| PyValueError::new_err("CDR encoding requires schema_text"))?;
            let type_name = type_name
                .ok_or_else(|| PyValueError::new_err("CDR encoding requires type_name"))?;

            let schema = parse_schema(type_name, schema).map_err(codec_error_to_py)?;
            let mut encoder = crate::encoding::CdrEncoder::new();
            encoder
                .encode_message(&decoded, &schema, type_name)
                .map_err(codec_error_to_py)?;
            let data = encoder.finish();
            (data, "cdr")
        }
        "json" => {
            let json_data = serde_json::to_string(&decoded)
                .map_err(|e| PyValueError::new_err(format!("Failed to encode as JSON: {}", e)))?;
            (json_data.into_bytes(), "json")
        }
        _ => {
            return Err(PyValueError::new_err(format!(
                "Unknown encoding: {} (supported: cdr, json)",
                encoding
            )))
        }
    };

    // Build metadata dict
    let metadata = PyDict::new(py);
    metadata.set_item("encoding", encoding_name)?;
    metadata.set_item("type_name", type_name.unwrap_or("unknown"))?;
    metadata.set_item("length", data.len())?;

    let bytes_obj = PyObject::from(data.as_slice().into_pyobject(py)?.to_owned());
    let metadata_obj = PyObject::from(metadata);

    Ok((bytes_obj, metadata_obj))
}

// =============================================================================
// Writer - Write messages to MCAP/BAG files
// =============================================================================

/// Writer for robotics data files (MCAP, ROS1 bag).
///
/// Auto-registers channels from the first message written to each topic.
///
/// Example:
///     >>> with robocodec.Writer("output.mcap") as writer:
///     ...     writer.write("/chatter", {"data": "hello"}, 1234567890,
///     ...                   schema_text="string data",
///     ...                   message_type="std_msgs/String")
#[pyclass(name = "Writer")]
pub struct PyWriter {
    writer: Option<crate::RoboWriter>,
    path: String,
    registered_channels: HashMap<String, (String, String)>, // topic -> (msg_type, schema)
}

#[pymethods]
impl PyWriter {
    /// Create a new writer (auto-detects format from extension).
    ///
    /// Args:
    ///     path: Output file path (.mcap or .bag)
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let writer = crate::RoboWriter::create(path).map_err(codec_error_to_py)?;

        Ok(Self {
            writer: Some(writer),
            path: path.to_string(),
            registered_channels: HashMap::new(),
        })
    }

    /// Write a message to a topic (auto-registers channel on first write).
    ///
    /// Args:
    ///     topic: Topic name (e.g., "/chatter")
    ///     message: Dictionary with message fields
    ///     timestamp_ns: Timestamp in nanoseconds
    ///     schema_text: Schema definition text (required on first write per topic)
    ///     message_type: Message type name (required on first write per topic)
    ///     encoding: Encoding format (default: "cdr")
    #[pyo3(keyword)]
    fn write(
        &mut self,
        py: Python<'_>,
        topic: &str,
        message: &Bound<'_, PyDict>,
        timestamp_ns: u64,
        schema_text: Option<&str>,
        message_type: Option<&str>,
        encoding: Option<&str>,
    ) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Writer is closed"))?;

        // Auto-register channel if not already registered
        if !self.registered_channels.contains_key(topic) {
            let schema = schema_text.ok_or_else(|| {
                PyValueError::new_err(format!(
                    "schema_text required for first write to topic '{}'",
                    topic
                ))
            })?;
            let msg_type = message_type.ok_or_else(|| {
                PyValueError::new_err(format!(
                    "message_type required for first write to topic '{}'",
                    topic
                ))
            })?;

            writer
                .add_channel(topic, msg_type, schema)
                .map_err(codec_error_to_py)?;

            self.registered_channels.insert(
                topic.to_string(),
                (msg_type.to_string(), schema.to_string()),
            );
        }

        // Encode the message
        let encoding = encoding.unwrap_or("cdr");
        let (data, _) = encode(
            py,
            message,
            schema_text.or_else(|| self.registered_channels.get(topic).map(|(_, s)| s.as_str())),
            message_type.or_else(|| self.registered_channels.get(topic).map(|(t, _)| t.as_str())),
            encoding,
        )?;

        let data_bytes: Vec<u8> = data.extract(py)?;

        // Write the encoded message
        writer
            .write_message(topic, &data_bytes, timestamp_ns)
            .map_err(codec_error_to_py)?;

        Ok(())
    }

    /// Close the writer and finalize the file.
    fn close(&mut self) -> PyResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.finish().map_err(codec_error_to_py)?;
        }
        Ok(())
    }

    /// Context manager entry.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit.
    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false) // Don't suppress exceptions
    }
}

// =============================================================================
// Python module definition
// =============================================================================

/// Robocodec Python module.
#[pymodule]
fn _robocodec(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Version info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Functions
    m.add_function(wrap_pyfunction!(parse_schema_text, m)?)?;
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    m.add_function(wrap_pyfunction!(transform, m)?)?;
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;

    // Classes
    m.add_class::<PyCdrDecoder>()?;
    m.add_class::<PyProtobufDecoder>()?;
    m.add_class::<PyJsonDecoder>()?;
    m.add_class::<PyReader>()?;
    m.add_class::<PyMessageIter>()?;
    m.add_class::<PyWriter>()?;

    Ok(())
}
