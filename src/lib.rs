mod decoder;
use decoder::PackStreamDecoder;
use pyo3::prelude::*;

#[pyfunction]
fn read_map(py: Python, bytes: Vec<u8>) -> PyResult<PyObject> {
    let mut decoder = PackStreamDecoder::new(bytes, &py);
    return Ok(decoder.read());
}

/// A Python module implemented in Rust.
#[pymodule]
fn fast_packstream(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_map, m)?)?;
    Ok(())
}