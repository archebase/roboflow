"""
Comprehensive tests for robocodec encode() and decode() functionality.

Tests cover:
- encode() function: CDR and JSON encoding for various types
- Nested types, arrays, structured messages
- Error handling and edge cases
- Round-trip encode → decode verification
"""

import os
import tempfile
import shutil

import pytest
import robocodec


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def temp_mcap_path():
    """Create a temporary MCAP file path and cleanup after test."""
    fd, path = tempfile.mkstemp(suffix=".mcap")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def temp_bag_path():
    """Create a temporary BAG file path and cleanup after test."""
    fd, path = tempfile.mkstemp(suffix=".bag")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def temp_dir():
    """Create a temporary directory and cleanup after test."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


# =============================================================================
# Schema Definitions
# =============================================================================

STD_MSGS_STRING_SCHEMA = "string data"
STD_MSGS_INT32_SCHEMA = "int32 data"
STD_MSGS_INT64_SCHEMA = "int64 data"
STD_MSGS_FLOAT32_SCHEMA = "float32 data"
STD_MSGS_FLOAT64_SCHEMA = "float64 data"
STD_MSGS_BOOL_SCHEMA = "bool data"

SENSOR_MSGS_IMAGE_SCHEMA = """
std_msgs/Header header
  uint32 seq
  time stamp
  string frame_id
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
"""

GEOMETRY_MSGS_TWIST_SCHEMA = """
geometry_msgs/Vector3 linear
  float64 x
  float64 y
  float64 z
geometry_msgs/Vector3 angular
  float64 x
  float64 y
  float64 z
"""

STD_MSGS_HEADER_SCHEMA = """
uint32 seq
time stamp
string frame_id
"""

NESTED_MSG_SCHEMA = """
std_msgs/Header header
  uint32 seq
  time stamp
  string frame_id
string name
int32 value
"""


# =============================================================================
# encode() Function Tests - Basic CDR Encoding
# =============================================================================


class TestEncodeBasicCdr:
    """Tests for basic CDR encoding with simple types."""

    def test_encode_string_message(self):
        """Test encoding a simple string message."""
        data, meta = robocodec.encode(
            {"data": "hello world"},
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert meta["encoding"] == "cdr"
        assert meta["type_name"] == "std_msgs/String"
        assert "length" in meta
        assert meta["length"] == len(data)

    def test_encode_int32_message(self):
        """Test encoding an int32 message."""
        data, meta = robocodec.encode(
            {"data": 42},
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) >= 4  # At least 4 bytes for int32

    def test_encode_int64_message(self):
        """Test encoding an int64 message."""
        data, meta = robocodec.encode(
            {"data": 9_223_372_036_854_775_807},
            schema_text=STD_MSGS_INT64_SCHEMA,
            type_name="std_msgs/Int64",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) >= 8  # At least 8 bytes for int64

    def test_encode_float32_message(self):
        """Test encoding a float32 message."""
        data, meta = robocodec.encode(
            {"data": 3.14},
            schema_text=STD_MSGS_FLOAT32_SCHEMA,
            type_name="std_msgs/Float32",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) >= 4

    def test_encode_float64_message(self):
        """Test encoding a float64 message."""
        data, meta = robocodec.encode(
            {"data": 2.718281828459045},
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) >= 8

    def test_encode_bool_message(self):
        """Test encoding a bool message."""
        data, meta = robocodec.encode(
            {"data": True},
            schema_text=STD_MSGS_BOOL_SCHEMA,
            type_name="std_msgs/Bool",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) >= 1


class TestEncodeNestedCdr:
    """Tests for CDR encoding with nested message types."""

    def test_encode_nested_message(self):
        """Test encoding a message with nested Header."""
        data, meta = robocodec.encode(
            {
                "header": {"seq": 123, "stamp": 1_234_567_890, "frame_id": "base_link"},
                "name": "test_joint",
                "value": 456,
            },
            schema_text=NESTED_MSG_SCHEMA,
            type_name="test_pkg/NestedMsg",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert meta["encoding"] == "cdr"

    def test_encode_geometry_twist(self):
        """Test encoding geometry_msgs/Twist with nested Vector3."""
        data, meta = robocodec.encode(
            {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 0.1, "y": 0.2, "z": 0.3},
            },
            schema_text=GEOMETRY_MSGS_TWIST_SCHEMA,
            type_name="geometry_msgs/Twist",
            encoding="cdr",
        )

        assert isinstance(data, bytes)
        assert len(data) > 0


# =============================================================================
# encode() Function Tests - JSON Encoding
# =============================================================================


class TestEncodeJson:
    """Tests for JSON encoding."""

    def test_encode_json_simple(self):
        """Test encoding a simple message as JSON."""
        data, meta = robocodec.encode(
            {"data": "hello"},
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="json",
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert meta["encoding"] == "json"
        # Should be valid JSON
        import json

        decoded = json.loads(data)
        assert decoded["data"] == "hello"

    def test_encode_json_nested(self):
        """Test encoding a nested message as JSON."""
        data, meta = robocodec.encode(
            {
                "header": {"seq": 123, "stamp": 1_234_567_890, "frame_id": "base_link"},
                "name": "test",
            },
            schema_text=NESTED_MSG_SCHEMA,
            type_name="test_pkg/NestedMsg",
            encoding="json",
        )

        assert isinstance(data, bytes)
        assert meta["encoding"] == "json"


# =============================================================================
# encode() Function Tests - Error Handling
# =============================================================================


class TestEncodeErrors:
    """Tests for encode() error handling."""

    def test_encode_cdr_without_schema(self):
        """Test that CDR encoding requires schema_text."""
        with pytest.raises(ValueError, match="schema_text"):
            robocodec.encode(
                {"data": "test"},
                schema_text=None,
                type_name="std_msgs/String",
                encoding="cdr",
            )

    def test_encode_cdr_without_type_name(self):
        """Test that CDR encoding requires type_name."""
        with pytest.raises(ValueError, match="type_name"):
            robocodec.encode(
                {"data": "test"},
                schema_text=STD_MSGS_STRING_SCHEMA,
                type_name=None,
                encoding="cdr",
            )

    def test_encode_unknown_encoding(self):
        """Test that unknown encoding raises an error."""
        with pytest.raises(ValueError, match="encoding"):
            robocodec.encode(
                {"data": "test"},
                schema_text=STD_MSGS_STRING_SCHEMA,
                type_name="std_msgs/String",
                encoding="unknown",
            )

    def test_encode_protobuf_not_implemented(self):
        """Test that protobuf encoding returns an appropriate error."""
        # Protobuf encoding may not be fully implemented yet
        # This test documents current behavior
        with pytest.raises(ValueError, match="encoding|unsupported"):
            robocodec.encode(
                {"data": "test"},
                schema_text=STD_MSGS_STRING_SCHEMA,
                type_name="std_msgs/String",
                encoding="protobuf",
            )


# =============================================================================
# encode() Function Tests - Round-Trip (encode → decode)
# =============================================================================


class TestEncodeRoundTrip:
    """Tests for encode → decode round-trip verification."""

    def test_round_trip_string(self):
        """Test encode → decode round-trip for string message."""
        original = {"data": "hello world"}

        # Encode
        data, _ = robocodec.encode(
            original,
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr",
        )

        # Decode
        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr",
        )

        assert decoded["data"] == original["data"]

    def test_round_trip_int32(self):
        """Test encode → decode round-trip for int32 message."""
        original = {"data": 12345}

        data, _ = robocodec.encode(
            original,
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr",
        )

        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr",
        )

        assert decoded["data"] == original["data"]

    def test_round_trip_float64(self):
        """Test encode → decode round-trip for float64 message."""
        original = {"data": 3.141592653589793}

        data, _ = robocodec.encode(
            original,
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr",
        )

        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr",
        )

        assert abs(decoded["data"] - original["data"]) < 1e-10

    def test_round_trip_json(self):
        """Test encode → decode round-trip for JSON encoding."""
        original = {"data": "test", "value": 42}

        data, _ = robocodec.encode(
            original,
            schema_text="string data\nint32 value",
            type_name="test/MultiField",
            encoding="json",
        )

        decoded = robocodec.decode(
            data,
            schema_text="string data\nint32 value",
            type_name="test/MultiField",
            encoding="json",
        )

        assert decoded["data"] == original["data"]
        assert decoded["value"] == original["value"]


# =============================================================================
# Edge Cases and Comprehensive Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_encode_empty_string(self):
        """Test encoding an empty string."""
        data, meta = robocodec.encode(
            {"data": ""},
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr",
        )
        assert isinstance(data, bytes)

    def test_encode_zero_values(self):
        """Test encoding zero values for numeric types."""
        data, meta = robocodec.encode(
            {"data": 0},
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr",
        )
        assert isinstance(data, bytes)

    def test_encode_negative_int(self):
        """Test encoding negative integer."""
        data, meta = robocodec.encode(
            {"data": -42},
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr",
        )
        assert isinstance(data, bytes)

    def test_encode_large_string(self):
        """Test encoding a large string."""
        large_string = "x" * 10000
        data, meta = robocodec.encode(
            {"data": large_string},
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr",
        )
        assert isinstance(data, bytes)
        assert len(data) > 10000

