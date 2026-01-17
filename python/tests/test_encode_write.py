"""
Comprehensive tests for robocodec encode() and Writer functionality.

Tests cover:
- encode() function: CDR and JSON encoding, nested types, arrays, errors
- Writer class: MCAP/BAG writing, auto-registration, context manager, round-trip
- Error handling and edge cases
"""

import os
import tempfile
import shutil
from pathlib import Path

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
            encoding="cdr"
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
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) >= 4  # At least 4 bytes for int32

    def test_encode_int64_message(self):
        """Test encoding an int64 message."""
        data, meta = robocodec.encode(
            {"data": 9_223_372_036_854_775_807},
            schema_text=STD_MSGS_INT64_SCHEMA,
            type_name="std_msgs/Int64",
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) >= 8  # At least 8 bytes for int64

    def test_encode_float32_message(self):
        """Test encoding a float32 message."""
        data, meta = robocodec.encode(
            {"data": 3.14},
            schema_text=STD_MSGS_FLOAT32_SCHEMA,
            type_name="std_msgs/Float32",
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) >= 4

    def test_encode_float64_message(self):
        """Test encoding a float64 message."""
        data, meta = robocodec.encode(
            {"data": 2.718281828459045},
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) >= 8

    def test_encode_bool_message(self):
        """Test encoding a bool message."""
        data, meta = robocodec.encode(
            {"data": True},
            schema_text=STD_MSGS_BOOL_SCHEMA,
            type_name="std_msgs/Bool",
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) >= 1


class TestEncodeNestedCdr:
    """Tests for CDR encoding with nested message types."""

    def test_encode_nested_message(self):
        """Test encoding a message with nested Header."""
        data, meta = robocodec.encode(
            {
                "header": {
                    "seq": 123,
                    "stamp": 1_234_567_890,
                    "frame_id": "base_link"
                },
                "name": "test_joint",
                "value": 456
            },
            schema_text=NESTED_MSG_SCHEMA,
            type_name="test_pkg/NestedMsg",
            encoding="cdr"
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert meta["encoding"] == "cdr"

    def test_encode_geometry_twist(self):
        """Test encoding geometry_msgs/Twist with nested Vector3."""
        data, meta = robocodec.encode(
            {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 0.1, "y": 0.2, "z": 0.3}
            },
            schema_text=GEOMETRY_MSGS_TWIST_SCHEMA,
            type_name="geometry_msgs/Twist",
            encoding="cdr"
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
            encoding="json"
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
                "header": {
                    "seq": 123,
                    "stamp": 1_234_567_890,
                    "frame_id": "base_link"
                },
                "name": "test"
            },
            schema_text=NESTED_MSG_SCHEMA,
            type_name="test_pkg/NestedMsg",
            encoding="json"
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
                encoding="cdr"
            )

    def test_encode_cdr_without_type_name(self):
        """Test that CDR encoding requires type_name."""
        with pytest.raises(ValueError, match="type_name"):
            robocodec.encode(
                {"data": "test"},
                schema_text=STD_MSGS_STRING_SCHEMA,
                type_name=None,
                encoding="cdr"
            )

    def test_encode_unknown_encoding(self):
        """Test that unknown encoding raises an error."""
        with pytest.raises(ValueError, match="encoding"):
            robocodec.encode(
                {"data": "test"},
                schema_text=STD_MSGS_STRING_SCHEMA,
                type_name="std_msgs/String",
                encoding="unknown"
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
                encoding="protobuf"
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
            encoding="cdr"
        )

        # Decode
        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr"
        )

        assert decoded["data"] == original["data"]

    def test_round_trip_int32(self):
        """Test encode → decode round-trip for int32 message."""
        original = {"data": 12345}

        data, _ = robocodec.encode(
            original,
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr"
        )

        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr"
        )

        assert decoded["data"] == original["data"]

    def test_round_trip_float64(self):
        """Test encode → decode round-trip for float64 message."""
        original = {"data": 3.141592653589793}

        data, _ = robocodec.encode(
            original,
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr"
        )

        decoded = robocodec.decode(
            data,
            schema_text=STD_MSGS_FLOAT64_SCHEMA,
            type_name="std_msgs/Float64",
            encoding="cdr"
        )

        assert abs(decoded["data"] - original["data"]) < 1e-10

    def test_round_trip_json(self):
        """Test encode → decode round-trip for JSON encoding."""
        original = {"data": "test", "value": 42}

        data, _ = robocodec.encode(
            original,
            schema_text="string data\nint32 value",
            type_name="test/MultiField",
            encoding="json"
        )

        decoded = robocodec.decode(
            data,
            schema_text="string data\nint32 value",
            type_name="test/MultiField",
            encoding="json"
        )

        assert decoded["data"] == original["data"]
        assert decoded["value"] == original["value"]


# =============================================================================
# Writer Class Tests - MCAP Writing
# =============================================================================

class TestWriterMcap:
    """Tests for Writer class with MCAP format."""

    def test_writer_create_mcap(self, temp_mcap_path):
        """Test creating a Writer for MCAP format."""
        writer = robocodec.Writer(temp_mcap_path)
        assert writer is not None
        # Writer should be open
        writer.close()

    def test_writer_mcap_context_manager(self, temp_mcap_path):
        """Test Writer as context manager for MCAP."""
        with robocodec.Writer(temp_mcap_path) as writer:
            assert writer is not None
        # File should be closed and finalized here
        assert os.path.exists(temp_mcap_path)

    def test_writer_mcap_single_message(self, temp_mcap_path):
        """Test writing a single message to MCAP."""
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic="/chatter",
                message={"data": "hello mcap"},
                timestamp_ns=1_234_567_890,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )

        # Verify file was created
        assert os.path.exists(temp_mcap_path)
        assert os.path.getsize(temp_mcap_path) > 0

    def test_writer_mcap_multiple_messages(self, temp_mcap_path):
        """Test writing multiple messages to MCAP."""
        with robocodec.Writer(temp_mcap_path) as writer:
            for i in range(10):
                writer.write(
                    topic="/chatter",
                    message={"data": f"message {i}"},
                    timestamp_ns=1_234_567_890 + i * 1_000_000,
                    schema_text=STD_MSGS_STRING_SCHEMA,
                    message_type="std_msgs/String"
                )

        # Verify file was created and has content
        assert os.path.exists(temp_mcap_path)
        assert os.path.getsize(temp_mcap_path) > 0

    def test_writer_mcap_multiple_topics(self, temp_mcap_path):
        """Test writing messages to multiple topics in MCAP."""
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic="/chatter",
                message={"data": "hello"},
                timestamp_ns=1_000_000_000,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )
            writer.write(
                topic="/status",
                message={"data": 42},
                timestamp_ns=1_000_001_000,
                schema_text=STD_MSGS_INT32_SCHEMA,
                message_type="std_msgs/Int32"
            )

        assert os.path.exists(temp_mcap_path)
        assert os.path.getsize(temp_mcap_path) > 0

    def test_writer_mcap_auto_register(self, temp_mcap_path):
        """Test that channels are auto-registered on first write."""
        with robocodec.Writer(temp_mcap_path) as writer:
            # First write should register the channel
            writer.write(
                topic="/auto_topic",
                message={"data": "test"},
                timestamp_ns=1_000_000_000,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )
            # Subsequent writes to same topic don't need schema info
            # (but we still provide them in this test)


# =============================================================================
# Writer Class Tests - BAG Writing
# =============================================================================

class TestWriterBag:
    """Tests for Writer class with BAG format."""

    def test_writer_create_bag(self, temp_bag_path):
        """Test creating a Writer for BAG format."""
        writer = robocodec.Writer(temp_bag_path)
        assert writer is not None
        writer.close()

    def test_writer_bag_context_manager(self, temp_bag_path):
        """Test Writer as context manager for BAG."""
        with robocodec.Writer(temp_bag_path) as writer:
            assert writer is not None
        assert os.path.exists(temp_bag_path)

    def test_writer_bag_single_message(self, temp_bag_path):
        """Test writing a single message to BAG."""
        with robocodec.Writer(temp_bag_path) as writer:
            writer.write(
                topic="/chatter",
                message={"data": "hello bag"},
                timestamp_ns=1_234_567_890,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )

        assert os.path.exists(temp_bag_path)
        assert os.path.getsize(temp_bag_path) > 0

    def test_writer_bag_multiple_messages(self, temp_bag_path):
        """Test writing multiple messages to BAG."""
        with robocodec.Writer(temp_bag_path) as writer:
            for i in range(10):
                writer.write(
                    topic="/chatter",
                    message={"data": f"bag message {i}"},
                    timestamp_ns=1_234_567_890 + i * 1_000_000,
                    schema_text=STD_MSGS_STRING_SCHEMA,
                    message_type="std_msgs/String"
                )

        assert os.path.exists(temp_bag_path)
        assert os.path.getsize(temp_bag_path) > 0


# =============================================================================
# Writer Class Tests - Round-Trip (write → read)
# =============================================================================

class TestWriterRoundTrip:
    """Tests for write → read round-trip verification."""

    def test_round_trip_mcap_single_message(self, temp_mcap_path):
        """Test write → read round-trip for MCAP with single message."""
        original_msg = {"data": "round_trip_test"}
        topic = "/test_topic"
        timestamp = 1_234_567_890

        # Write
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic=topic,
                message=original_msg,
                timestamp_ns=timestamp,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )

        # Read back
        reader = robocodec.read(temp_mcap_path)
        channels = reader.channels()

        # Verify channel exists (channels returns a list)
        assert len(channels) > 0
        assert any(c["topic"] == topic for c in channels)

        # Verify message
        found = False
        for msg, channel in reader.iter_messages():
            if channel["topic"] == topic:
                assert msg["data"] == original_msg["data"]
                found = True
                break

        assert found, "Message not found in round-trip"

    def test_round_trip_mcap_multiple_messages(self, temp_mcap_path):
        """Test write → read round-trip for MCAP with multiple messages."""
        messages = [
            {"data": f"message_{i}"}
            for i in range(5)
        ]
        topic = "/multi_test"

        # Write
        with robocodec.Writer(temp_mcap_path) as writer:
            for i, msg in enumerate(messages):
                writer.write(
                    topic=topic,
                    message=msg,
                    timestamp_ns=1_000_000_000 + i * 1_000_000,
                    schema_text=STD_MSGS_STRING_SCHEMA,
                    message_type="std_msgs/String"
                )

        # Read back
        reader = robocodec.read(temp_mcap_path)
        retrieved = []
        for msg, channel in reader.iter_messages():
            if channel["topic"] == topic:
                retrieved.append(msg)

        assert len(retrieved) == len(messages)
        for i, msg in enumerate(retrieved):
            assert msg["data"] == messages[i]["data"]

    def test_round_trip_bag_single_message(self, temp_bag_path):
        """Test write → read round-trip for BAG with single message."""
        original_msg = {"data": "bag_round_trip"}
        topic = "/bag_test"
        timestamp = 9_876_543_210

        # Write
        with robocodec.Writer(temp_bag_path) as writer:
            writer.write(
                topic=topic,
                message=original_msg,
                timestamp_ns=timestamp,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )

        # Read back
        reader = robocodec.read(temp_bag_path)
        channels = reader.channels()

        # Verify channel exists (channels returns a list)
        assert len(channels) > 0

        # Verify message
        found = False
        for msg, channel in reader.iter_messages():
            if channel["topic"] == topic:
                assert msg["data"] == original_msg["data"]
                found = True
                break

        assert found, "Message not found in round-trip"


# =============================================================================
# Writer Class Tests - Error Handling
# =============================================================================

class TestWriterErrors:
    """Tests for Writer error handling."""

    def test_writer_unknown_format(self, temp_dir):
        """Test that unknown file format raises an error."""
        unknown_path = os.path.join(temp_dir, "test.unknown_format")

        with pytest.raises(ValueError, match="format|unknown"):
            robocodec.Writer(unknown_path)

    def test_writer_write_without_registration(self, temp_mcap_path):
        """Test that writing without schema info on first write raises an error."""
        with pytest.raises(ValueError, match="schema_text"):
            with robocodec.Writer(temp_mcap_path) as writer:
                writer.write(
                    topic="/test",
                    message={"data": "test"},
                    timestamp_ns=1_000_000_000,
                    # Missing schema_text and message_type
                    schema_text=None,
                    message_type=None
                )

    def test_writer_write_after_close(self, temp_mcap_path):
        """Test that writing to a closed writer raises an error."""
        writer = robocodec.Writer(temp_mcap_path)
        writer.close()

        with pytest.raises(RuntimeError):
            writer.write(
                topic="/test",
                message={"data": "test"},
                timestamp_ns=1_000_000_000,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )


# =============================================================================
# Writer Class Tests - Auto-Registration
# =============================================================================

class TestWriterAutoRegistration:
    """Tests for channel auto-registration feature."""

    def test_auto_register_first_write_needs_schema(self, temp_mcap_path):
        """Test that first write requires schema for auto-registration."""
        with pytest.raises(ValueError, match="schema_text"):
            with robocodec.Writer(temp_mcap_path) as writer:
                writer.write(
                    topic="/new_topic",
                    message={"data": "test"},
                    timestamp_ns=1_000_000_000
                    # No schema_text or message_type
                )

    def test_auto_register_subsequent_writes_require_schema(self, temp_mcap_path):
        """Test that subsequent writes still require schema (current behavior)."""
        with robocodec.Writer(temp_mcap_path) as writer:
            # First write with schema (registers channel)
            writer.write(
                topic="/topic1",
                message={"data": "first"},
                timestamp_ns=1_000_000_000,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )

            # Second write to same topic still requires schema in current implementation
            with pytest.raises(ValueError, match="schema_text"):
                writer.write(
                    topic="/topic1",
                    message={"data": "second"},
                    timestamp_ns=1_000_001_000
                    # No schema - should fail in current implementation
                )


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
            encoding="cdr"
        )
        assert isinstance(data, bytes)

    def test_encode_zero_values(self):
        """Test encoding zero values for numeric types."""
        data, meta = robocodec.encode(
            {"data": 0},
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr"
        )
        assert isinstance(data, bytes)

    def test_encode_negative_int(self):
        """Test encoding negative integer."""
        data, meta = robocodec.encode(
            {"data": -42},
            schema_text=STD_MSGS_INT32_SCHEMA,
            type_name="std_msgs/Int32",
            encoding="cdr"
        )
        assert isinstance(data, bytes)

    def test_encode_large_string(self):
        """Test encoding a large string."""
        large_string = "x" * 10000
        data, meta = robocodec.encode(
            {"data": large_string},
            schema_text=STD_MSGS_STRING_SCHEMA,
            type_name="std_msgs/String",
            encoding="cdr"
        )
        assert isinstance(data, bytes)
        assert len(data) > 10000

    def test_writer_empty_message(self, temp_mcap_path):
        """Test writing a message with empty string."""
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic="/empty",
                message={"data": ""},
                timestamp_ns=1_000_000_000,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )
        assert os.path.exists(temp_mcap_path)

    def test_writer_zero_timestamp(self, temp_mcap_path):
        """Test writing a message with zero timestamp."""
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic="/zero_time",
                message={"data": "test"},
                timestamp_ns=0,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )
        assert os.path.exists(temp_mcap_path)

    def test_writer_very_large_timestamp(self, temp_mcap_path):
        """Test writing a message with very large timestamp."""
        large_timestamp = 2**63 - 1  # Max int64
        with robocodec.Writer(temp_mcap_path) as writer:
            writer.write(
                topic="/large_time",
                message={"data": "test"},
                timestamp_ns=large_timestamp,
                schema_text=STD_MSGS_STRING_SCHEMA,
                message_type="std_msgs/String"
            )
        assert os.path.exists(temp_mcap_path)
