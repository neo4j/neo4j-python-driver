use pyo3::prelude::*;
use pyo3::types::{PyDict, IntoPyDict};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn read_map(py: Python, bytes: Vec<u8>) -> PyResult<&PyDict> {
    let mut kvps: Vec<(String, PyObject)> = Vec::new();
    kvps.push(("Hello".to_string(), 10.to_object(py)));
    kvps.push(("Hello2".to_string(), 10f32.to_object(py)));
    kvps.push(("Hello3".to_string(), PyObject::from(PyDict::new(py))));
    Ok(kvps.into_py_dict(py))
}

/// A Python module implemented in Rust.
#[pymodule]
fn fast_packstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_map, m)?)?;
    Ok(())
}

struct PackStreamDecoder<'a> {
    bytes: Vec<u8>,
    py: Python<'a>,
    index: usize
}

impl <'b> PackStreamDecoder<'b> {
    pub fn new<'a>(data: Vec<u8>, py: &'a Python<'b>) -> PackStreamDecoder<'a> {
        Self { bytes: data, py: *py, index: 0 }
    }

    pub fn read(&mut self) -> PyObject {

    }


}