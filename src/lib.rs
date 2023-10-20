mod decoder;
use decoder::PackStreamDecoder;
use pyo3::prelude::*;
use pyo3::types::PyByteArray;

#[pyfunction]
unsafe fn read(py: Python, bytes: &PyByteArray) -> PyResult<PyObject> {
    let mut decoder = PackStreamDecoder::new(bytes.as_bytes(), &py);
    let result = decoder.read();
    return Ok(result);
}

/// A Python module implemented in Rust.
#[pymodule]
fn fast_packstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    Ok(())
}
