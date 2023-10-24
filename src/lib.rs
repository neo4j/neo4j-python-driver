pub mod v1;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

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
pub struct Structure {
    tag: u8,
    #[pyo3(get)]
    pub fields: Vec<PyObject>,
}

#[pymethods]
impl Structure {
    #[getter(tag)]
    fn read_tag<'a>(&self, py: Python<'a>) -> &'a PyBytes {
        PyBytes::new(py, &[self.tag])
    }
}
