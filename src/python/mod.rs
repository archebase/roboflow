// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for roboflow.
//!
//! Minimal, clean bindings that expose roboflow's high-level fluent API.

mod fluent;

use pyo3::prelude::*;

use fluent::{
    PyBatchReport, PyCompressionPreset, PyFileResult, PyHyperPipelineReport, PyPipelineReport,
    PyRobocodec, PyTransformBuilder,
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

    Ok(())
}
