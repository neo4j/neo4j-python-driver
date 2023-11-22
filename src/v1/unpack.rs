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

use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyByteArray, PyBytes, PyDict, PyList, PyTuple};

use super::{
    BYTES_16, BYTES_32, BYTES_8, FALSE, FLOAT_64, INT_16, INT_32, INT_64, INT_8, LIST_16, LIST_32,
    LIST_8, MAP_16, MAP_32, MAP_8, NULL, STRING_16, STRING_32, STRING_8, TINY_LIST, TINY_MAP,
    TINY_STRING, TINY_STRUCT, TRUE,
};
use crate::Structure;

#[pyfunction]
pub(super) fn unpack(
    py: Python,
    bytes: &PyByteArray,
    idx: usize,
    hydration_hooks: Option<&PyDict>,
) -> PyResult<(PyObject, usize)> {
    let mut decoder = PackStreamDecoder::new(bytes, py, idx, hydration_hooks);
    let result = decoder.read()?;
    Ok((result, decoder.index))
}

struct PackStreamDecoder<'a> {
    bytes: &'a PyByteArray,
    py: Python<'a>,
    index: usize,
    hydration_hooks: Option<&'a PyDict>,
}

impl<'a> PackStreamDecoder<'a> {
    fn new(
        bytes: &'a PyByteArray,
        py: Python<'a>,
        idx: usize,
        hydration_hooks: Option<&'a PyDict>,
    ) -> Self {
        Self {
            bytes,
            py,
            index: idx,
            hydration_hooks,
        }
    }

    fn read(&mut self) -> PyResult<PyObject> {
        let marker = self.read_byte()?;
        self.read_value(marker)
    }

    fn read_value(&mut self, marker: u8) -> PyResult<PyObject> {
        let high_nibble = marker & 0xF0;

        Ok(match marker {
            // tiny int
            _ if marker as i8 >= -16 => (marker as i8).to_object(self.py),
            NULL => self.py.None(),
            FLOAT_64 => self.read_f64()?.to_object(self.py),
            FALSE => false.to_object(self.py),
            TRUE => true.to_object(self.py),
            INT_8 => self.read_i8()?.to_object(self.py),
            INT_16 => self.read_i16()?.to_object(self.py),
            INT_32 => self.read_i32()?.to_object(self.py),
            INT_64 => self.read_i64()?.to_object(self.py),
            BYTES_8 => {
                let len = self.read_u8()?;
                self.read_bytes(len)?
            }
            BYTES_16 => {
                let len = self.read_u16()?;
                self.read_bytes(len)?
            }
            BYTES_32 => {
                let len = self.read_u32()?;
                self.read_bytes(len)?
            }
            _ if high_nibble == TINY_STRING => self.read_string((marker & 0x0F).into())?,
            STRING_8 => {
                let len = self.read_u8()?;
                self.read_string(len)?
            }
            STRING_16 => {
                let len = self.read_u16()?;
                self.read_string(len)?
            }
            STRING_32 => {
                let len = self.read_u32()?;
                self.read_string(len)?
            }
            _ if high_nibble == TINY_LIST => self.read_list((marker & 0x0F).into())?,
            LIST_8 => {
                let len = self.read_u8()?;
                self.read_list(len)?
            }
            LIST_16 => {
                let len = self.read_u16()?;
                self.read_list(len)?
            }
            LIST_32 => {
                let len = self.read_u32()?;
                self.read_list(len)?
            }
            _ if high_nibble == TINY_MAP => self.read_map((marker & 0x0F).into())?,
            MAP_8 => {
                let len = self.read_u8()?;
                self.read_map(len)?
            }
            MAP_16 => {
                let len = self.read_u16()?;
                self.read_map(len)?
            }
            MAP_32 => {
                let len = self.read_u32()?;
                self.read_map(len)?
            }
            _ if high_nibble == TINY_STRUCT => self.read_struct((marker & 0x0F).into())?,
            _ => {
                // raise ValueError("Unknown PackStream marker %02X" % marker)
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "Unknown PackStream marker {:02X}",
                    marker
                )));
            }
        })
    }

    fn read_list(&mut self, length: usize) -> PyResult<PyObject> {
        if length == 0 {
            return Ok(PyList::empty(self.py).to_object(self.py));
        }
        let mut items = Vec::with_capacity(length);
        for _ in 0..length {
            items.push(self.read()?);
        }
        Ok(items.to_object(self.py))
    }

    fn read_string(&mut self, length: usize) -> PyResult<PyObject> {
        if length == 0 {
            return Ok("".to_object(self.py));
        }
        let data = unsafe {
            // Safety: we're holding the GIL, and don't interact with Python while using the bytes
            let data = &self.bytes.as_bytes()[self.index..self.index + length];
            // We have to copy the data to uphold the safety invariant.
            String::from_utf8(data.into())
                .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?
        };
        self.index += length;
        Ok(data.to_object(self.py))
    }

    fn read_map(&mut self, length: usize) -> PyResult<PyObject> {
        if length == 0 {
            return Ok(PyDict::new(self.py).to_object(self.py));
        }
        let mut key_value_pairs: Vec<(PyObject, PyObject)> = Vec::with_capacity(length);
        for _ in 0..length {
            let len = self.read_string_length()?;
            let key = self.read_string(len)?;
            let value = self.read()?;
            key_value_pairs.push((key, value));
        }
        Ok(key_value_pairs.into_py_dict(self.py).into())
    }

    fn read_bytes(&mut self, length: usize) -> PyResult<PyObject> {
        if length == 0 {
            return Ok(PyBytes::new(self.py, &[]).to_object(self.py));
        }
        let data = unsafe {
            // Safety: we're holding the GIL, and don't interact with Python while using the bytes.
            // We have to copy the data to uphold the safety invariant.
            self.bytes.as_bytes()[self.index..self.index + length].to_vec()
        };
        self.index += length;
        Ok(PyBytes::new(self.py, &data).to_object(self.py))
    }

    fn read_struct(&mut self, length: usize) -> PyResult<PyObject> {
        let tag = self.read_byte()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.read()?)
        }
        let mut bolt_struct = Structure { tag, fields }.into_py(self.py);
        let Some(hooks) = self.hydration_hooks else {
            return Ok(bolt_struct);
        };

        let attr = bolt_struct.getattr(self.py, intern!(self.py, "__class__"))?;
        if let Some(res) = hooks.get_item(attr) {
            bolt_struct = res
                .call(PyTuple::new(self.py, [bolt_struct]), None)?
                .into_py(self.py);
        }

        Ok(bolt_struct)
    }

    fn read_string_length(&mut self) -> PyResult<usize> {
        let marker = self.read_byte()?;
        let high_nibble = marker & 0xF0;
        match high_nibble {
            TINY_STRING => Ok((marker & 0x0F) as usize),
            STRING_8 => self.read_u8(),
            STRING_16 => self.read_u16(),
            STRING_32 => self.read_u32(),
            _ => Err(PyErr::new::<PyValueError, _>(format!(
                "Invalid string length marker: {}",
                marker
            ))),
        }
    }

    fn read_byte(&mut self) -> PyResult<u8> {
        let byte = unsafe {
            // Safety: we're holding the GIL, and don't interact with Python while using the bytes
            *self
                .bytes
                .as_bytes()
                .get(self.index)
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Nothing to unpack"))?
        };
        self.index += 1;
        Ok(byte)
    }

    fn read_n_bytes<const N: usize>(&mut self) -> PyResult<[u8; N]> {
        let to = self.index + N;
        unsafe {
            // Safety: we're holding the GIL, and don't interact with Python while using the bytes.
            match self.bytes.as_bytes().get(self.index..to) {
                Some(b) => {
                    self.index = to;
                    Ok(<[u8; N]>::try_from(b).expect("we know the slice has exactly N values"))
                }
                None => Err(PyErr::new::<PyValueError, _>("Nothing to unpack")),
            }
        }
    }

    fn read_u8(&mut self) -> PyResult<usize> {
        self.read_byte().map(Into::into)
    }

    fn read_u16(&mut self) -> PyResult<usize> {
        let data = self.read_n_bytes()?;
        Ok(u16::from_be_bytes(data).into())
    }

    fn read_u32(&mut self) -> PyResult<usize> {
        let data = self.read_n_bytes()?;
        u32::from_be_bytes(data).try_into().map_err(|_| {
            PyErr::new::<PyValueError, _>(
                "Server announced 32 bit sized data. Not supported by this architecture.",
            )
        })
    }

    fn read_i8(&mut self) -> PyResult<i8> {
        self.read_byte().map(|b| i8::from_be_bytes([b]))
    }

    fn read_i16(&mut self) -> PyResult<i16> {
        self.read_n_bytes().map(i16::from_be_bytes)
    }

    fn read_i32(&mut self) -> PyResult<i32> {
        self.read_n_bytes().map(i32::from_be_bytes)
    }

    fn read_i64(&mut self) -> PyResult<i64> {
        self.read_n_bytes().map(i64::from_be_bytes)
    }

    fn read_f64(&mut self) -> PyResult<f64> {
        self.read_n_bytes().map(f64::from_be_bytes)
    }
}
