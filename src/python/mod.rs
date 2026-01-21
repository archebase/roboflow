//! Python bindings for robocodec.
//!
//! Thin PyO3 wrappers that expose existing robocodec APIs to Python.
//! Uses the new I/O layer with RoboReader/RoboWriter.

mod fluent;

use crate::{
    encoding::{cdr::CdrDecoder, json::JsonDecoder, protobuf::ProtobufDecoder},
    io::{
        metadata::RawMessage,
        traits::{FormatReader, FormatWriter},
        ReaderBuilder, RoboReader, RoboWriter,
    },
    rewriter::RewriteOptions,
    schema::parse_schema,
    transform::TransformBuilder,
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
// Message iterator - MCAP only for now
// =============================================================================

/// Iterator for decoded messages from MCAP files.
///
/// Note: This is a simplified implementation that works with MCAP files.
#[pyclass(name = "MessageIter", unsendable)]
pub struct PyMessageIter {
    #[allow(dead_code)]
    reader: Option<crate::format::McapReader>,
    #[allow(dead_code)]
    channels: std::collections::HashMap<u16, crate::format::ChannelInfo>,
    #[allow(dead_code)]
    cdr_decoder: std::sync::Arc<crate::encoding::CdrDecoder>,
    #[allow(dead_code)]
    proto_decoder: std::sync::Arc<crate::encoding::protobuf::ProtobufDecoder>,
    #[allow(dead_code)]
    json_decoder: std::sync::Arc<crate::encoding::json::JsonDecoder>,
    #[allow(dead_code)]
    message_index: usize,
    #[allow(dead_code)]
    messages: Vec<(crate::format::RawMessage, crate::format::ChannelInfo)>,
}

impl PyMessageIter {
    /// Return the next decoded message as a tuple (message_dict, channel_info_dict).
    /// Returns None when exhausted.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        use crate::Decoder;

        if slf.message_index >= slf.messages.len() {
            return Ok(None);
        }

        // Get the index first, then increment to avoid borrow conflicts
        let idx = slf.message_index;
        slf.message_index += 1;

        let (raw_msg, channel) = &slf.messages[idx];

        let schema = channel.schema.as_deref().unwrap_or("");
        let type_name = Some(channel.message_type.as_str());

        // Decode the message based on encoding
        // Dereference Arc to access the Decoder trait implementation
        let decoded = match channel.encoding.as_str() {
            "cdr" | "cdr0" => Decoder::decode(&*slf.cdr_decoder, &raw_msg.data, schema, type_name),
            "protobuf" => Decoder::decode(&*slf.proto_decoder, &raw_msg.data, schema, type_name),
            "json" | "jsonschema" => {
                Decoder::decode(&*slf.json_decoder, &raw_msg.data, schema, type_name)
            }
            _ => Err(crate::core::CodecError::parse(
                "PyMessageIter",
                format!("Unsupported encoding: {}", channel.encoding),
            )),
        }
        .map_err(codec_error_to_py)?;

        let msg_obj = codec_value_to_py(&CodecValue::Struct(decoded), py)?;
        let channel_obj = channel_info_to_py(channel, py)?;
        let tuple = (msg_obj, channel_obj);
        Ok(Some(PyObject::from(tuple.into_pyobject(py)?.to_owned())))
    }
}

// =============================================================================
// Reader - MCAP files
// =============================================================================

/// Reader for robotics data files (MCAP only for now).
///
/// Uses the new RoboReader from the I/O layer.
#[pyclass(name = "Reader", subclass)]
pub struct PyReader {
    reader: RoboReader,
}

#[pymethods]
impl PyReader {
    /// Open a robotics data file (MCAP only for now).
    ///
    /// Args:
    ///     path: Path to MCAP file
    ///
    /// Returns:
    ///     Reader instance
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let reader = ReaderBuilder::new()
            .path(path)
            .build()
            .map_err(codec_error_to_py)?;
        Ok(Self { reader })
    }

    /// Get information about all channels/topics in the file.
    ///
    /// Returns:
    ///     List of channel info dictionaries
    fn channels(&self, py: Python<'_>) -> PyResult<PyObject> {
        let channels = self.reader.channels();
        let list = PyList::empty(py);

        for channel in channels.values() {
            // Convert io::ChannelInfo to format::ChannelInfo for Python
            let ch_info = ChannelInfo {
                id: channel.id,
                topic: channel.topic.clone(),
                message_type: channel.message_type.clone(),
                encoding: channel.encoding.clone(),
                schema: channel.schema.clone(),
                schema_data: channel.schema_data.clone(),
                schema_encoding: channel.schema_encoding.clone(),
                message_count: channel.message_count,
                callerid: channel.callerid.clone(),
            };
            list.append(channel_info_to_py(&ch_info, py)?)?;
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
        use std::sync::Arc;

        // Get the format-specific reader
        let path = std::path::Path::new(self.reader.path());
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        if extension != "mcap" {
            return Err(PyValueError::new_err(
                "Message iteration currently only supported for MCAP files ",
            ));
        }

        let mcap_reader = crate::format::McapReader::open(path).map_err(codec_error_to_py)?;

        // Collect all raw messages with their channel info
        let mut messages = Vec::new();
        let raw_iter = mcap_reader.iter_raw().map_err(codec_error_to_py)?;
        let stream = raw_iter.into_stream().map_err(codec_error_to_py)?;

        for result in stream {
            match result {
                Ok((msg, channel)) => {
                    messages.push((msg, channel.clone()));
                }
                Err(e) => return Err(codec_error_to_py(e)),
            }
        }

        // Get channels and create decoders
        let channels = mcap_reader.channels().clone();
        let cdr_decoder = Arc::new(crate::encoding::CdrDecoder::new());
        let proto_decoder = Arc::new(crate::encoding::protobuf::ProtobufDecoder::new());
        let json_decoder = Arc::new(crate::encoding::json::JsonDecoder::new());

        Ok(PyMessageIter {
            reader: Some(mcap_reader),
            channels,
            cdr_decoder,
            proto_decoder,
            json_decoder,
            message_index: 0,
            messages,
        })
    }
}

// =============================================================================
// Writer - MCAP/BAG writing
// =============================================================================

/// Writer for robotics data files (MCAP, BAG).
///
/// Automatically detects format based on file extension.
/// Messages are buffered and written as a batch when close() is called.
#[pyclass(name = "Writer", unsendable)]
pub struct PyWriter {
    writer: Option<RoboWriter>,
    path: String,
    channel_registry: std::collections::HashMap<String, u16>,
    message_buffer: Vec<RawMessage>,
}

#[pymethods]
impl PyWriter {
    /// Create a new writer for a robotics data file.
    ///
    /// Args:
    ///     path: Path to output file (.mcap or .bag)
    ///
    /// Returns:
    ///     Writer instance
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let writer = RoboWriter::create(path).map_err(codec_error_to_py)?;
        Ok(Self {
            writer: Some(writer),
            path: path.to_string(),
            channel_registry: std::collections::HashMap::new(),
            message_buffer: Vec::new(),
        })
    }

    /// Write a message to the file.
    ///
    /// Args:
    ///     topic: Topic name (e.g., "/chatter")
    ///     message: Python dict with message fields
    ///     timestamp_ns: Timestamp in nanoseconds
    ///     schema_text: Schema definition text (required for first message on topic)
    ///     message_type: Message type name (e.g., "std_msgs/String")
    ///     encoding: Encoding format ("cdr", "json", "protobuf"). Defaults to "cdr"
    ///
    /// Raises:
    ///     RuntimeError: If writer is closed or write fails
    #[pyo3(signature = (topic, message, timestamp_ns, schema_text=None, message_type=None, encoding=None))]
    fn write(
        &mut self,
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

        let encoding = encoding.unwrap_or("cdr");
        let message_type = message_type.unwrap_or("Unknown");

        // Register channel if needed
        let channel_id = if let Some(&id) = self.channel_registry.get(topic) {
            id
        } else {
            let id = writer
                .add_channel(topic, message_type, encoding, schema_text)
                .map_err(codec_error_to_py)?;
            self.channel_registry.insert(topic.to_string(), id);
            id
        };

        // Encode message
        let decoded = py_dict_to_decoded_message(message)?;
        let data = match encoding {
            "cdr" => {
                let schema = schema_text.ok_or_else(|| {
                    PyValueError::new_err("CDR encoding requires schema_text")
                })?;
                let schema = parse_schema(message_type, schema).map_err(codec_error_to_py)?;
                let mut encoder = crate::encoding::CdrEncoder::new();
                encoder
                    .encode_message(&decoded, &schema, message_type)
                    .map_err(codec_error_to_py)?;
                encoder.finish()
            }
            "json" => {
                // Use JsonDecoder's encode method which properly serializes CodecValue
                let json_encoder = JsonDecoder::new();
                let json_str = json_encoder
                    .encode(&decoded, false)
                    .map_err(codec_error_to_py)?;
                json_str.into_bytes()
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported encoding: {}",
                    encoding
                )))
            }
        };

        // Create raw message and buffer it
        let raw_msg = RawMessage {
            channel_id,
            sequence: Some(0),
            log_time: timestamp_ns,
            publish_time: timestamp_ns,
            data,
        };

        // Buffer the message for batch writing on close
        self.message_buffer.push(raw_msg);
        Ok(())
    }

    /// Close and finalize the file.
    fn close(&mut self) -> PyResult<()> {
        if let Some(mut writer) = self.writer.take() {
            // Write all buffered messages as a batch
            if !self.message_buffer.is_empty() {
                writer
                    .write_batch(&self.message_buffer)
                    .map_err(codec_error_to_py)?;
                self.message_buffer.clear();
            }
            writer.finish().map_err(codec_error_to_py)?;
        }
        Ok(())
    }

    /// Get the number of messages buffered (waiting to be written).
    fn message_count(&self) -> PyResult<u64> {
        Ok(self.message_buffer.len() as u64)
    }

    /// Get the number of channels registered.
    fn channel_count(&self) -> PyResult<usize> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Writer is closed"))?;
        Ok(writer.channel_count())
    }

    /// Context manager entry.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit.
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, pyo3::PyAny>>,
        _exc_value: Option<&Bound<'_, pyo3::PyAny>>,
        _traceback: Option<&Bound<'_, pyo3::PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
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
///     convert("input.bag ", "output.mcap ", rename={"/old ": "/new "})
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
///     path: Path to MCAP file
///
/// Returns:
///     Reader instance
#[pyfunction]
fn read(path: &str) -> PyResult<PyReader> {
    let reader = ReaderBuilder::new()
        .path(path)
        .build()
        .map_err(codec_error_to_py)?;
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
                    "CDR decoding requires schema_text and type_name ",
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
///     type_name: Message type name (e.g., "std_msgs/String ")
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
///     ...     schema_text="string data ",
///     ...     type_name="std_msgs/String ",
///     ...     encoding="cdr"
///     ... )
///     >>> print(f"Encoded {len(data)} bytes ")
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
                .ok_or_else(|| PyValueError::new_err("CDR encoding requires schema_text "))?;
            let type_name = type_name
                .ok_or_else(|| PyValueError::new_err("CDR encoding requires type_name "))?;

            let schema = parse_schema(type_name, schema).map_err(codec_error_to_py)?;
            let mut encoder = crate::encoding::CdrEncoder::new();
            encoder
                .encode_message(&decoded, &schema, type_name)
                .map_err(codec_error_to_py)?;
            let data = encoder.finish();
            (data, "cdr")
        }
        "json" => {
            // Use JsonDecoder's encode method which properly serializes CodecValue
            let json_encoder = JsonDecoder::new();
            let json_data = json_encoder
                .encode(&decoded, false)
                .map_err(codec_error_to_py)?;
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

    // Classes - Decoders
    m.add_class::<PyCdrDecoder>()?;
    m.add_class::<PyProtobufDecoder>()?;
    m.add_class::<PyJsonDecoder>()?;

    // Classes - Reader
    m.add_class::<PyReader>()?;
    m.add_class::<PyMessageIter>()?;

    // Classes - Writer
    m.add_class::<PyWriter>()?;

    // Classes - Fluent API
    m.add_class::<fluent::PyRobocodec>()?;
    m.add_class::<fluent::PyCompressionPreset>()?;
    m.add_class::<fluent::PyTransformBuilder>()?;
    m.add_class::<fluent::PyPipelineReport>()?;
    m.add_class::<fluent::PyHyperPipelineReport>()?;
    m.add_class::<fluent::PyFileResult>()?;
    m.add_class::<fluent::PyBatchReport>()?;

    Ok(())
}
