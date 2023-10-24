use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyByteArray, PyDict, PyList, PyTuple};

use crate::Structure;

const TINY_STRING: u8 = 0x80;
const TINY_LIST: u8 = 0x90;
const TINY_MAP: u8 = 0xA0;
const TINY_STRUCT: u8 = 0xB0;
const NULL: u8 = 0xC0;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const INT_8: u8 = 0xC8;
const INT_16: u8 = 0xC9;
const INT_32: u8 = 0xCA;
const INT_64: u8 = 0xCB;
const FLOAT_64: u8 = 0xC1;
const STRING_8: u8 = 0xD0;
const STRING_16: u8 = 0xD1;
const STRING_32: u8 = 0xD2;
const LIST_8: u8 = 0xD4;
const LIST_16: u8 = 0xD5;
const LIST_32: u8 = 0xD6;
const MAP_8: u8 = 0xD8;
const MAP_16: u8 = 0xD9;
const MAP_32: u8 = 0xDA;
const STRUCT_8: u8 = 0xDC;
const STRUCT_16: u8 = 0xDD;

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(unpack, m)?)
}

#[pyfunction]
unsafe fn unpack(
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
    pub fn new(
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

    pub fn read(&mut self) -> PyResult<PyObject> {
        let marker = self.read_byte()?;
        self.read_value(marker)
    }

    fn read_value(&mut self, marker: u8) -> PyResult<PyObject> {
        let high_nibble = marker & 0xF0;
        Ok(match high_nibble {
            TINY_STRING => self.read_string((marker & 0x0F) as usize)?,
            TINY_LIST => self.read_list((marker & 0x0F) as usize)?,
            TINY_MAP => self.read_map((marker & 0x0F) as usize)?,
            TINY_STRUCT => self.read_struct((marker & 0x0F) as usize)?,
            _ if marker as i8 >= -16i8 => i8::try_from(marker)
                .expect("checked marker is in bounds")
                .to_object(self.py),
            _ => match marker {
                NULL => self.py.None(),
                FALSE => false.to_object(self.py),
                TRUE => true.to_object(self.py),
                INT_8 => self.read_i8()?.to_object(self.py),
                INT_16 => self.read_i16()?.to_object(self.py),
                INT_32 => self.read_i32()?.to_object(self.py),
                INT_64 => self.read_i64()?.to_object(self.py),
                FLOAT_64 => self.read_f64()?.to_object(self.py),
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
                STRUCT_8 => {
                    let len = self.read_u8()?;
                    self.read_struct(len)?
                }
                STRUCT_16 => {
                    let len = self.read_u16()?;
                    self.read_struct(len)?
                }
                _ => panic!("Invalid marker: {}", marker),
            },
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
            // we have to copy the data to uphold the safety invariant
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

    fn read_struct(&mut self, length: usize) -> PyResult<PyObject> {
        let tag = self.read_byte()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.read()?)
        }
        let mut bolt_struct = Structure { tag, fields }.into_py(self.py);
        let Some(hooks) = dbg!(self.hydration_hooks) else {
            return Ok(bolt_struct);
        };

        let attr = bolt_struct.getattr(self.py, "__class__")?;
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

    fn read_2_bytes(&mut self) -> PyResult<[u8; 2]> {
        Ok([self.read_byte()?, self.read_byte()?])
    }

    fn read_4_bytes(&mut self) -> PyResult<[u8; 4]> {
        Ok([
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
        ])
    }

    fn read_8_bytes(&mut self) -> PyResult<[u8; 8]> {
        Ok([
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
            self.read_byte()?,
        ])
    }

    fn read_u8(&mut self) -> PyResult<usize> {
        self.read_byte().map(Into::into)
    }

    fn read_u16(&mut self) -> PyResult<usize> {
        let data = self.read_2_bytes()?;
        Ok(u16::from_be_bytes(data).into())
    }

    fn read_u32(&mut self) -> PyResult<usize> {
        let data = self.read_4_bytes()?;
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
        self.read_2_bytes().map(i16::from_be_bytes)
    }

    fn read_i32(&mut self) -> PyResult<i32> {
        self.read_4_bytes().map(i32::from_be_bytes)
    }

    fn read_i64(&mut self) -> PyResult<i64> {
        self.read_8_bytes().map(i64::from_be_bytes)
    }

    fn read_f64(&mut self) -> PyResult<f64> {
        self.read_8_bytes().map(f64::from_be_bytes)
    }
}
