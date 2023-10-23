mod decoder;

use decoder::PackStreamDecoder;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict};

#[pyfunction]
unsafe fn read(
    py: Python,
    bytes: &PyByteArray,
    idx: usize,
    hydration_hooks: Option<&PyDict>,
) -> PyResult<(PyObject, usize)> {
    let mut decoder = PackStreamDecoder::new(bytes.as_bytes(), py, idx, hydration_hooks);
    let result = decoder.read();
    Ok((result, decoder.index))
}

/// A Python module implemented in Rust.
#[pymodule]
fn fast_packstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_class::<BoltStruct>()?;
    Ok(())
}

#[pyclass]
pub struct BoltStruct {
    tag: u8,
    #[pyo3(get)]
    pub fields: Vec<PyObject>,
}

#[pymethods]
impl BoltStruct {
    #[getter(tag)]
    fn read_tag<'a>(&self, py: Python<'a>) -> &'a PyBytes {
        PyBytes::new(py, &[self.tag])
    }
}
