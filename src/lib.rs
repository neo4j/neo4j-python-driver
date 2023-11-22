// Copyright (c) "Neo4j"
// Neo4j Sweden AB [https://neo4j.com]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod v1;

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_rust")]
fn packstream(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Structure>()?;

    let mod_v1 = PyModule::new(py, "v1")?;
    v1::register(py, mod_v1)?;
    m.add_submodule(mod_v1)?;
    register_package(py, mod_v1, "v1")?;

    Ok(())
}

// hack to make python pick up the submodule as a package
// https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
fn register_package(py: Python, m: &PyModule, name: &str) -> PyResult<()> {
    let locals = PyDict::new(py);
    locals.set_item("module", m)?;
    py.run(
        &format!("import sys; sys.modules['neo4j._codec.packstream._rust.{name}'] = module"),
        None,
        Some(locals),
    )
}

#[pyclass]
#[derive(Debug)]
pub struct Structure {
    tag: u8,
    #[pyo3(get)]
    fields: Vec<PyObject>,
}

#[pymethods]
impl Structure {
    #[new]
    #[pyo3(signature = (tag, *fields))]
    #[pyo3(text_signature = "(tag, *fields)")]
    fn new(tag: &[u8], fields: Vec<PyObject>) -> PyResult<Self> {
        if tag.len() != 1 {
            return Err(PyErr::new::<PyValueError, _>("tag must be a single byte"));
        }
        let tag = tag[0];
        Ok(Self { tag, fields })
    }

    #[getter(tag)]
    fn read_tag<'a>(&self, py: Python<'a>) -> &'a PyBytes {
        PyBytes::new(py, &[self.tag])
    }

    #[getter(fields)]
    fn read_fields<'a>(&self, py: Python<'a>) -> &'a PyTuple {
        PyTuple::new(py, &self.fields)
    }

    fn eq(&self, other: &Self, py: Python<'_>) -> PyResult<bool> {
        if self.tag != other.tag || self.fields.len() != other.fields.len() {
            return Ok(false);
        }
        for (a, b) in self
            .fields
            .iter()
            .map(|e| e.as_ref(py))
            .zip(other.fields.iter().map(|e| e.as_ref(py)))
        {
            if !a.eq(b)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp, py: Python<'_>) -> PyResult<PyObject> {
        Ok(match op {
            CompareOp::Eq => self.eq(other, py)?.into_py(py),
            CompareOp::Ne => (!self.eq(other, py)?).into_py(py),
            _ => py.NotImplemented(),
        })
    }

    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        let mut fields_hash = 0;
        for field in &self.fields {
            fields_hash += field.as_ref(py).hash()?;
        }
        Ok(fields_hash.wrapping_add(self.tag.into()))
    }
}
