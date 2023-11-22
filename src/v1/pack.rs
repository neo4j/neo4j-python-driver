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

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::{PyImportError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBytes, PyDict, PyString, PyType};

use super::{
    BYTES_16, BYTES_32, BYTES_8, FALSE, FLOAT_64, INT_16, INT_32, INT_64, INT_8, LIST_16, LIST_32,
    LIST_8, MAP_16, MAP_32, MAP_8, NULL, STRING_16, STRING_32, STRING_8, TINY_LIST, TINY_MAP,
    TINY_STRING, TINY_STRUCT, TRUE,
};
use crate::Structure;

#[derive(Debug)]
struct TypeMappings {
    none_values: Vec<PyObject>,
    true_values: Vec<PyObject>,
    false_values: Vec<PyObject>,
    int_types: PyObject,
    float_types: PyObject,
    sequence_types: PyObject,
    mapping_types: PyObject,
    bytes_types: PyObject,
}

impl TypeMappings {
    fn new(py: Python<'_>, locals: &PyDict) -> PyResult<Self> {
        Ok(Self {
            none_values: locals
                .get_item("NONE_VALUES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing NONE_VALUES.")
                })?
                .extract()?,
            true_values: locals
                .get_item("TRUE_VALUES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing TRUE_VALUES.")
                })?
                .extract()?,
            false_values: locals
                .get_item("FALSE_VALUES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing FALSE_VALUES.")
                })?
                .extract()?,
            int_types: locals
                .get_item("INT_TYPES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing INT_TYPES.")
                })?
                .into_py(py),
            float_types: locals
                .get_item("FLOAT_TYPES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing FLOAT_TYPES.")
                })?
                .into_py(py),
            sequence_types: locals
                .get_item("SEQUENCE_TYPES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing SEQUENCE_TYPES.")
                })?
                .into_py(py),
            mapping_types: locals
                .get_item("MAPPING_TYPES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing MAPPING_TYPES.")
                })?
                .into_py(py),
            bytes_types: locals
                .get_item("BYTES_TYPES")
                .ok_or_else(|| {
                    PyErr::new::<PyValueError, _>("Type mappings are missing BYTES_TYPES.")
                })?
                .into_py(py),
        })
    }
}

static TYPE_MAPPINGS: GILOnceCell<PyResult<TypeMappings>> = GILOnceCell::new();
static TYPE_MAPPINGS_INIT: AtomicBool = AtomicBool::new(false);

fn get_type_mappings(py: Python<'_>) -> PyResult<&'static TypeMappings> {
    let mappings = TYPE_MAPPINGS.get_or_try_init(py, || {
        fn init(py: Python<'_>) -> PyResult<TypeMappings> {
            let locals = PyDict::new(py);
            py.run(
                "from neo4j._codec.packstream.v1.types import *",
                None,
                Some(locals),
            )?;
            TypeMappings::new(py, locals)
        }

        if TYPE_MAPPINGS_INIT.swap(true, Ordering::SeqCst) {
            return Err(PyErr::new::<PyImportError, _>(
                "Cannot call _rust.pack while loading `neo4j._codec.packstream.v1.types`",
            ));
        }
        Ok(init(py))
    });
    mappings?.as_ref().map_err(|e| e.clone_ref(py))
}

#[pyfunction]
pub(super) fn pack<'py>(
    py: Python<'py>,
    value: &PyAny,
    dehydration_hooks: Option<&PyAny>,
) -> PyResult<&'py PyBytes> {
    let type_mappings = get_type_mappings(py)?;
    let mut encoder = PackStreamEncoder::new(py, dehydration_hooks, type_mappings);
    encoder.write(value)?;
    Ok(PyBytes::new(py, &encoder.buffer))
}

struct PackStreamEncoder<'a> {
    py: Python<'a>,
    dehydration_hooks: Option<&'a PyAny>,
    type_mappings: &'a TypeMappings,
    buffer: Vec<u8>,
}

impl<'a> PackStreamEncoder<'a> {
    fn new(
        py: Python<'a>,
        dehydration_hooks: Option<&'a PyAny>,
        type_mappings: &'a TypeMappings,
    ) -> Self {
        Self {
            py,
            dehydration_hooks,
            type_mappings,
            buffer: Default::default(),
        }
    }

    fn write(&mut self, value: &PyAny) -> PyResult<()> {
        if self.write_exact_value(value, &self.type_mappings.none_values, &[NULL])? {
            return Ok(());
        }
        if self.write_exact_value(value, &self.type_mappings.true_values, &[TRUE])? {
            return Ok(());
        }
        if self.write_exact_value(value, &self.type_mappings.false_values, &[FALSE])? {
            return Ok(());
        }

        if value.is_instance(self.type_mappings.float_types.as_ref(self.py))? {
            let value = value.extract::<f64>()?;
            return self.write_float(value);
        }

        if value.is_instance(self.type_mappings.int_types.as_ref(self.py))? {
            let value = value.extract::<i64>()?;
            return self.write_int(value);
        }

        if value.is_instance(PyType::new::<PyString>(self.py))? {
            return self.write_string(value.extract::<&str>()?);
        }

        if value.is_instance(self.type_mappings.bytes_types.as_ref(self.py))? {
            return self.write_bytes(value.extract::<Cow<[u8]>>()?);
        }

        if value.is_instance(self.type_mappings.sequence_types.as_ref(self.py))? {
            let size = Self::usize_to_u64(value.len()?)?;
            self.write_list_header(size)?;
            return value.iter()?.try_for_each(|item| self.write(item?));
        }

        if value.is_instance(self.type_mappings.mapping_types.as_ref(self.py))? {
            let size = Self::usize_to_u64(value.getattr("keys")?.call0()?.len()?)?;
            self.write_dict_header(size)?;
            let items = value.getattr(intern!(self.py, "items"))?.call0()?;
            return items.iter()?.try_for_each(|item| {
                let (key, value) = item?.extract::<(&PyAny, &PyAny)>()?;
                let key = match key.extract::<&str>() {
                    Ok(key) => key,
                    Err(_) => {
                        return Err(PyErr::new::<PyTypeError, _>(format!(
                            "Map keys must be strings, not {}",
                            key.get_type().str()?
                        )))
                    }
                };
                self.write_string(key)?;
                self.write(value)
            });
        }

        if let Ok(value) = value.extract::<&PyCell<Structure>>() {
            let value_ref = value.borrow();
            let size = value_ref.fields.len().try_into().map_err(|_| {
                PyErr::new::<PyOverflowError, _>("Structure header size out of range")
            })?;
            self.write_struct_header(value_ref.tag, size)?;
            return value_ref
                .fields
                .iter()
                .try_for_each(|item| self.write(item.as_ref(self.py)));
        }

        if let Some(dehydration_hooks) = self.dehydration_hooks {
            let transformer =
                dehydration_hooks.call_method1(intern!(self.py, "get_transformer"), (value,))?;
            if !transformer.is_none() {
                let value = transformer.call1((value,))?;
                return self.write(value);
            }
        }

        // raise ValueError("Values of type %s are not supported" % type(value))
        Err(PyErr::new::<PyValueError, _>(format!(
            "Values of type {} are not supported",
            value.get_type().str()?
        )))
    }

    fn write_exact_value(
        &mut self,
        value: &PyAny,
        values: &[PyObject],
        bytes: &[u8],
    ) -> PyResult<bool> {
        for v in values {
            if value.is(v) {
                self.buffer.extend(bytes);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn write_int(&mut self, i: i64) -> PyResult<()> {
        if (-16..=127).contains(&i) {
            self.buffer.extend(&i8::to_be_bytes(i as i8));
        } else if (-128..=127).contains(&i) {
            self.buffer.extend(&[INT_8]);
            self.buffer.extend(&i8::to_be_bytes(i as i8));
        } else if (-32_768..=32_767).contains(&i) {
            self.buffer.extend(&[INT_16]);
            self.buffer.extend(&i16::to_be_bytes(i as i16));
        } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
            self.buffer.extend(&[INT_32]);
            self.buffer.extend(&i32::to_be_bytes(i as i32));
        } else {
            self.buffer.extend(&[INT_64]);
            self.buffer.extend(&i64::to_be_bytes(i));
        }
        Ok(())
    }

    fn write_float(&mut self, f: f64) -> PyResult<()> {
        self.buffer.extend(&[FLOAT_64]);
        self.buffer.extend(&f64::to_be_bytes(f));
        Ok(())
    }

    fn write_bytes(&mut self, b: Cow<[u8]>) -> PyResult<()> {
        let size = Self::usize_to_u64(b.len())?;
        if size <= 255 {
            self.buffer.extend(&[BYTES_8]);
            self.buffer.extend(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.buffer.extend(&[BYTES_16]);
            self.buffer.extend(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.buffer.extend(&[BYTES_32]);
            self.buffer.extend(&u32::to_be_bytes(size as u32));
        } else {
            return Err(PyErr::new::<PyOverflowError, _>(
                "Bytes header size out of range",
            ));
        }
        self.buffer.extend(b.iter());
        Ok(())
    }

    fn usize_to_u64(size: usize) -> PyResult<u64> {
        u64::try_from(size).map_err(|e| PyErr::new::<PyOverflowError, _>(e.to_string()))
    }

    fn write_string(&mut self, s: &str) -> PyResult<()> {
        let bytes = s.as_bytes();
        let size = Self::usize_to_u64(bytes.len())?;
        if size <= 15 {
            self.buffer.extend(&[TINY_STRING + size as u8]);
        } else if size <= 255 {
            self.buffer.extend(&[STRING_8]);
            self.buffer.extend(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.buffer.extend(&[STRING_16]);
            self.buffer.extend(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.buffer.extend(&[STRING_32]);
            self.buffer.extend(&u32::to_be_bytes(size as u32));
        } else {
            return Err(PyErr::new::<PyOverflowError, _>(
                "String header size out of range",
            ));
        }
        self.buffer.extend(bytes);
        Ok(())
    }

    fn write_list_header(&mut self, size: u64) -> PyResult<()> {
        if size <= 15 {
            self.buffer.extend(&[TINY_LIST + size as u8]);
        } else if size <= 255 {
            self.buffer.extend(&[LIST_8]);
            self.buffer.extend(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.buffer.extend(&[LIST_16]);
            self.buffer.extend(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.buffer.extend(&[LIST_32]);
            self.buffer.extend(&u32::to_be_bytes(size as u32));
        } else {
            return Err(PyErr::new::<PyOverflowError, _>(
                "List header size out of range",
            ));
        }
        Ok(())
    }

    fn write_dict_header(&mut self, size: u64) -> PyResult<()> {
        if size <= 15 {
            self.buffer.extend(&[TINY_MAP + size as u8]);
        } else if size <= 255 {
            self.buffer.extend(&[MAP_8]);
            self.buffer.extend(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.buffer.extend(&[MAP_16]);
            self.buffer.extend(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.buffer.extend(&[MAP_32]);
            self.buffer.extend(&u32::to_be_bytes(size as u32));
        } else {
            return Err(PyErr::new::<PyOverflowError, _>(
                "Map header size out of range",
            ));
        }
        Ok(())
    }

    fn write_struct_header(&mut self, tag: u8, size: u8) -> PyResult<()> {
        if size > 15 {
            return Err(PyErr::new::<PyOverflowError, _>(
                "Structure size out of range",
            ));
        }
        self.buffer.extend(&[TINY_STRUCT + size, tag]);
        Ok(())
    }
}
