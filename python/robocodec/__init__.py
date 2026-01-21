"""Robocodec - Schema-driven robotics message codec.

Supports decoding CDR (ROS1/ROS2), Protobuf, and JSON message formats,
and reading/writing MCAP and ROS1 bag files.

Example:
    >>> import robocodec
    >>>
    >>> # Convert BAG to MCAP with topic rename
    >>> robocodec.convert("input.bag", "output.mcap", rename={"/old": "/new"})
    >>>
    >>> # Use the fluent API for more control
    >>> from robocodec import Robocodec, CompressionPreset
    >>> result = Robocodec.open(["input.bag"])
    ...     .write_to("output.mcap")
    ...     .with_compression(CompressionPreset.fast())
    ...     .run()
    >>>
    >>> # Read and iterate messages
    >>> reader = robocodec.read("data.mcap")
    >>> for msg, channel in reader.iter_messages():
    ...     print(f"Message from {channel['topic']}: {msg}")
"""

from robocodec._robocodec import (
    __version__,
    BatchReport,
    CdrDecoder,
    CompressionPreset,
    FileResult,
    HyperPipelineReport,
    JsonDecoder,
    MessageIter,
    PipelineReport,
    ProtobufDecoder,
    Reader as _Reader,
    Robocodec,
    Writer,
    _TransformBuilder,
    convert as _convert,
    decode as _decode,
    encode as _encode,
    parse_schema_text,
    transform as _transform,
)

__all__ = [
    "__version__",
    # Fluent API
    "Robocodec",
    "CompressionPreset",
    "TransformBuilder",
    # Pipeline Reports
    "PipelineReport",
    "HyperPipelineReport",
    "BatchReport",
    "FileResult",
    # Convenience functions
    "convert",
    "transform",
    "read",
    "decode",
    "encode",
    # Decoders
    "CdrDecoder",
    "ProtobufDecoder",
    "JsonDecoder",
    # Reader
    "Reader",
    "MessageIter",
    # Writer
    "Writer",
    # Schema
    "parse_schema_text",
]


# Re-export with friendly names
TransformBuilder = _TransformBuilder

# Re-export Reader class
class Reader(_Reader):
    """Reader for robotics data files (MCAP, ROS1 bag).

    Example:
        >>> reader = robocodec.Reader("data.mcap")
        >>> for channel in reader.channels():
        ...     print(f"{channel['topic']}: {channel['message_count']} messages")
        >>>
        >>> # Iterate over decoded messages
        >>> for msg, channel in reader.iter_messages():
        ...     print(f"Message from {channel['topic']}: {msg}")
    """

    pass


def convert(
    input_path: str,
    output_path: str,
    rename: dict[str, str] | None = None,
    type_rename: dict[str, str] | None = None,
) -> None:
    """Convert between robotics data formats (BAG ↔ MCAP) with optional transforms.

    Args:
        input_path: Input file path (.bag or .mcap)
        output_path: Output file path
        rename: Optional dict mapping old topics to new topics
        type_rename: Optional dict mapping old types to new types

    Example:
        >>> # BAG to MCAP with topic rename
        >>> robocodec.convert("input.bag", "output.mcap", rename={"/old": "/new"})
        >>>
        >>> # MCAP to MCAP with type rename
        >>> robocodec.convert("in.mcap", "out.mcap", type_rename={"OldType": "NewType"})
    """
    _convert(input_path, output_path, rename, type_rename)


def transform(
    input_path: str,
    output_path: str | None = None,
    rename: dict[str, str] | None = None,
    type_rename: dict[str, str] | None = None,
) -> None:
    """Transform an existing file (alias for convert with same format).

    Args:
        input_path: Input file path
        output_path: Output file path (None = in-place, overwrites input)
        rename: Optional dict mapping old topics to new topics
        type_rename: Optional dict mapping old types to new types

    Example:
        >>> # Transform in-place (overwrites input)
        >>> robocodec.transform("data.mcap", rename={"/old": "/new"})
    """
    _transform(input_path, output_path, rename, type_rename)


def read(path: str) -> Reader:
    """Open a robotics data file for reading.

    Args:
        path: Path to MCAP or BAG file

    Returns:
        Reader instance

    Example:
        >>> reader = robocodec.read("data.mcap")
        >>> print(f"Total messages: {reader.message_count()}")
        >>> for channel in reader.channels():
        ...     print(f"{channel['topic']}: {channel['message_type']}")
    """
    return _Reader(path)


def decode(
    data: bytes,
    schema_text: str | None = None,
    type_name: str | None = None,
    encoding: str = "cdr",
) -> dict:
    """Decode binary data with schema.

    Args:
        data: Binary data to decode
        schema_text: Schema definition text (required for CDR)
        type_name: Message type name (required for CDR)
        encoding: Encoding format ("cdr", "protobuf", or "json")

    Returns:
        Dictionary with decoded fields

    Example:
        >>> # Decode CDR
        >>> msg = robocodec.decode(
        ...     data,
        ...     schema_text="int32 value\\nfloat64 data",
        ...     type_name="TestMsg",
        ...     encoding="cdr"
        ... )
        >>> print(msg)  # {'value': 42, 'data': 3.14}
        >>>
        >>> # Decode JSON
        >>> msg = robocodec.decode(b'{"x": 1, "y": 2}', encoding="json")
    """
    return _decode(data, schema_text, type_name, encoding)


def encode(
    message: dict,
    schema_text: str,
    type_name: str,
    encoding: str = "cdr",
) -> tuple[bytes, dict]:
    """Encode a Python dict to binary message.

    Args:
        message: Dictionary with message fields
        schema_text: Schema definition text (required for CDR)
        type_name: Message type name (e.g., "std_msgs/String")
        encoding: Encoding format ("cdr" or "json")

    Returns:
        Tuple of (encoded_bytes, metadata_dict) where metadata contains:
        - "encoding": The encoding format used
        - "type_name": The message type name
        - "length": Number of bytes in encoded data

    Example:
        >>> data, meta = robocodec.encode(
        ...     {"data": "hello world"},
        ...     schema_text="string data",
        ...     type_name="std_msgs/String",
        ...     encoding="cdr"
        ... )
        >>> print(f"Encoded {len(data)} bytes using {meta['encoding']}")
    """
    return _encode(message, schema_text, type_name, encoding)
