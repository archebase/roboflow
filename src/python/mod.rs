// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for roboflow.
//!
//! Minimal, clean bindings that expose roboflow's high-level fluent API.

mod dataset;
mod fluent;

use pyo3::prelude::*;

use fluent::{
    PyBatchReport, PyCompressionPreset, PyFileResult, PyHyperPipelineReport, PyPipelineReport,
    PyRobocodec, PyTransformBuilder,
};

use dataset::{
    PyConversionJob, PyDatasetConfig, PyDatasetConverter, PyDatasetStats, PyProgressUpdate,
};

/// Python module definition
#[pymodule]
fn _roboflow(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Main fluent API classes
    m.add_class::<PyRobocodec>()?;
    m.add_class::<PyTransformBuilder>()?;
    m.add_class::<PyCompressionPreset>()?;

    // Result and report classes
    m.add_class::<PyPipelineReport>()?;
    m.add_class::<PyHyperPipelineReport>()?;
    m.add_class::<PyFileResult>()?;
    m.add_class::<PyBatchReport>()?;

    // Dataset API classes
    m.add_class::<PyDatasetConverter>()?;
    m.add_class::<PyDatasetConfig>()?;
    m.add_class::<PyConversionJob>()?;
    m.add_class::<PyDatasetStats>()?;
    m.add_class::<PyProgressUpdate>()?;
    m.add_function(wrap_pyfunction!(convert, m)?)?;

    Ok(())
}

/// Simple conversion function.
///
/// Direct conversion without creating a converter object.
#[pyfunction]
fn convert(
    py: Python<'_>,
    input_path: String,
    output_dir: String,
    config: &PyDatasetConfig,
) -> PyResult<PyDatasetStats> {
    let converter = PyDatasetConverter::create(output_dir, config)?;
    converter.convert(py, input_path)
}
