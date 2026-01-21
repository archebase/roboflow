"""
Tests for the robocodec fluent API.

Tests cover:
- open() function and Robocodec builder
- write_to() output configuration
- hyper_mode() for high throughput
- with_compression() for compression presets
- TransformBuilder for topic/type renaming
- Report types returned by run()
"""

import os
import shutil
import tempfile

import pytest
import robocodec


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def test_mcap_file():
    """Return path to a test MCAP file."""
    path = "tests/fixtures/robocodec_test_0.mcap"
    if not os.path.exists(path):
        pytest.skip(f"Test fixture not found: {path}")
    return path


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for output files."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


# =============================================================================
# Test: Module exports
# =============================================================================


class TestModuleExports:
    """Tests for module-level exports."""

    def test_version(self):
        """Test that __version__ is exported."""
        assert hasattr(robocodec, "__version__")
        assert isinstance(robocodec.__version__, str)

    def test_open_function(self):
        """Test that open() is exported at module level."""
        assert hasattr(robocodec, "open")
        assert callable(robocodec.open)

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        expected = [
            "__version__",
            "open",
            "Robocodec",
            "CompressionPreset",
            "TransformBuilder",
            "PipelineReport",
            "HyperPipelineReport",
            "FileResult",
            "BatchReport",
        ]
        for name in expected:
            assert name in robocodec.__all__, f"{name} not in __all__"


# =============================================================================
# Test: open() function
# =============================================================================


class TestOpenFunction:
    """Tests for the open() entry point."""

    def test_open_returns_builder(self, test_mcap_file):
        """Test that open() returns a Robocodec builder."""
        builder = robocodec.open([test_mcap_file])
        assert builder is not None

    def test_open_missing_file(self):
        """Test that open() raises error for missing file."""
        with pytest.raises(OSError):
            robocodec.open(["nonexistent_file.mcap"])

    def test_open_empty_list(self):
        """Test that open() raises error for empty file list."""
        with pytest.raises(ValueError):
            robocodec.open([])


# =============================================================================
# Test: Standard pipeline
# =============================================================================


class TestStandardPipeline:
    """Tests for standard pipeline mode."""

    def test_basic_conversion(self, test_mcap_file, temp_output_dir):
        """Test basic MCAP to MCAP conversion."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = robocodec.open([test_mcap_file]).write_to(output_file).run()

        assert isinstance(result, robocodec.PipelineReport)
        assert os.path.exists(output_file)
        assert result.message_count > 0

    def test_report_properties(self, test_mcap_file, temp_output_dir):
        """Test that PipelineReport has expected properties."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = robocodec.open([test_mcap_file]).write_to(output_file).run()

        assert hasattr(result, "input_file")
        assert hasattr(result, "output_file")
        assert hasattr(result, "input_size_bytes")
        assert hasattr(result, "output_size_bytes")
        assert hasattr(result, "duration_seconds")
        assert hasattr(result, "average_throughput_mb_s")
        assert hasattr(result, "compression_ratio")
        assert hasattr(result, "message_count")


# =============================================================================
# Test: Parallel pipeline
# =============================================================================


class TestHyperPipeline:
    """Tests for hyper pipeline mode."""

    def test_hyper_mode(self, test_mcap_file, temp_output_dir):
        """Test hyper mode conversion."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = (
            robocodec.open([test_mcap_file]).write_to(output_file).hyper_mode().run()
        )

        assert isinstance(result, robocodec.HyperPipelineReport)
        assert os.path.exists(output_file)

    def test_hyper_report_properties(self, test_mcap_file, temp_output_dir):
        """Test that HyperPipelineReport has expected properties."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = (
            robocodec.open([test_mcap_file]).write_to(output_file).hyper_mode().run()
        )

        assert hasattr(result, "input_file")
        assert hasattr(result, "output_file")
        assert hasattr(result, "throughput_mb_s")
        assert hasattr(result, "compression_ratio")
        assert hasattr(result, "message_count")


# =============================================================================
# Test: Compression presets
# =============================================================================


class TestCompressionPresets:
    """Tests for compression preset configuration."""

    def test_fast_preset(self):
        """Test CompressionPreset.fast()."""
        preset = robocodec.CompressionPreset.fast()
        assert "Fast" in str(preset)

    def test_balanced_preset(self):
        """Test CompressionPreset.balanced()."""
        preset = robocodec.CompressionPreset.balanced()
        assert "Balanced" in str(preset)

    def test_slow_preset(self):
        """Test CompressionPreset.slow()."""
        preset = robocodec.CompressionPreset.slow()
        assert "Slow" in str(preset)

    def test_with_compression(self, test_mcap_file, temp_output_dir):
        """Test conversion with compression preset."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = (
            robocodec.open([test_mcap_file])
            .write_to(output_file)
            .with_compression(robocodec.CompressionPreset.fast())
            .run()
        )

        assert isinstance(result, robocodec.PipelineReport)
        assert os.path.exists(output_file)


# =============================================================================
# Test: TransformBuilder
# =============================================================================


class TestTransformBuilder:
    """Tests for TransformBuilder."""

    def test_create_builder(self):
        """Test creating a TransformBuilder."""
        builder = robocodec.TransformBuilder()
        assert builder is not None

    def test_topic_rename(self):
        """Test adding topic rename."""
        builder = robocodec.TransformBuilder()
        builder = builder.with_topic_rename("/old_topic", "/new_topic")
        assert builder is not None

    def test_type_rename(self):
        """Test adding type rename."""
        builder = robocodec.TransformBuilder()
        builder = builder.with_type_rename("old_type", "new_type")
        assert builder is not None

    def test_build(self):
        """Test building transform pipeline."""
        builder = robocodec.TransformBuilder()
        builder = builder.with_topic_rename("/old", "/new")
        transform_id = builder.build()
        assert isinstance(transform_id, int)


# =============================================================================
# Test: Method chaining
# =============================================================================


class TestMethodChaining:
    """Tests for fluent method chaining."""

    def test_full_chain(self, test_mcap_file, temp_output_dir):
        """Test complete method chain."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = (
            robocodec.open([test_mcap_file])
            .write_to(output_file)
            .with_compression(robocodec.CompressionPreset.balanced())
            .with_threads(4)
            .run()
        )

        assert result is not None
        assert os.path.exists(output_file)

    def test_hyper_mode_with_compression(self, test_mcap_file, temp_output_dir):
        """Test hyper mode with compression."""
        output_file = os.path.join(temp_output_dir, "output.mcap")

        result = (
            robocodec.open([test_mcap_file])
            .write_to(output_file)
            .hyper_mode()
            .with_compression(robocodec.CompressionPreset.fast())
            .run()
        )

        assert isinstance(result, robocodec.HyperPipelineReport)
