use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict, PyList};
use std::str;
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

pub(crate) struct PackStreamDecoder<'a> {
    bytes: Vec<u8>,
    py: Python<'a>,
    index: usize,
}

impl<'b> PackStreamDecoder<'b> {
    pub fn new<'a>(data: Vec<u8>, py: &'a Python<'b>) -> PackStreamDecoder<'a> {
        Self {
            bytes: data,
            py: *py,
            index: 0,
        }
    }

    pub fn read(&mut self) -> PyObject {
        let marker = self.bytes[self.index];
        self.index += 1;
        return self.read_value(marker);
    }

    fn read_value(&mut self, marker: u8) -> PyObject {
        let high_nibble = marker & 0xF0;
        match high_nibble {
            TINY_STRING => self.read_string((marker & 0x0F) as usize),
            TINY_LIST => self.read_list((marker & 0x0F) as usize),
            TINY_MAP => self.read_map((marker & 0x0F) as usize),
            _ => {
                if marker as i8 >= -16i8 {
                    return (marker as i8).to_object(self.py);
                }
                match marker {
                    NULL => self.py.None(),
                    FALSE => false.to_object(self.py),
                    TRUE => true.to_object(self.py),
                    INT_8 => self.next_i8().to_object(self.py),
                    INT_16 => self.next_i16().to_object(self.py),
                    INT_32 => self.next_i32().to_object(self.py),
                    INT_64 => self.next_i64().to_object(self.py),
                    FLOAT_64 => self.read_double().to_object(self.py),
                    STRING_8 => {
                        let len = self.read_u8() as usize;
                        self.read_string(len)
                    }
                    STRING_16 => {
                        let len = self.read_u16() as usize;
                        self.read_string(len)
                    }
                    STRING_32 => {
                        let len = self.read_u32() as usize;
                        self.read_string(len)
                    }
                    LIST_8 => {
                        let len = self.read_u8() as usize;
                        self.read_list(len)
                    }
                    LIST_16 => {
                        let len = self.read_u16() as usize;
                        self.read_list(len)
                    }
                    LIST_32 => {
                        let len = self.read_u32() as usize;
                        self.read_list(len)
                    }
                    MAP_8 => {
                        let len = self.read_u8() as usize;
                        self.read_map(len)
                    }
                    MAP_16 => {
                        let len = self.read_u16() as usize;
                        self.read_map(len)
                    }
                    MAP_32 => {
                        let len = self.read_u32() as usize;
                        self.read_map(len)
                    }
                    _ => panic!("Invalid marker: {}", marker),
                }
            }
        }
    }

    fn read_list(&mut self, length: usize) -> PyObject {
        if length == 0 {
            return PyList::empty(self.py).to_object(self.py);
        }
        let mut items = Vec::with_capacity(length);
        for _ in 0..length {
            items.push(self.read());
        }
        return items.to_object(self.py);
    }

    fn read_string(&mut self, length: usize) -> PyObject {
        if length == 0 {
            return "".to_object(self.py);
        }
        let data = &self.bytes[self.index..self.index + length];
        self.index += length;
        return str::from_utf8(data).unwrap().to_object(self.py);
    }

    fn read_map(&mut self, length: usize) -> PyObject {
        if length == 0 {
            return PyDict::new(self.py).to_object(self.py);
        }

        let mut kvps: Vec<(PyObject, PyObject)> = Vec::with_capacity(length);
        for _ in 0..length {
            let len = self.read_string_length();
            let key = self.read_string(len);
            let value = self.read();
            kvps.push((key, value));
        }
        return kvps.into_py_dict(self.py).into();
    }

    fn read_string_length(&mut self) -> usize {
        let marker = self.bytes[self.index];
        self.index += 1;
        let high_nibble = marker & 0xF0;
        match high_nibble {
            TINY_STRING => (marker & 0x0F) as usize,
            STRING_8 => self.read_u8() as usize,
            STRING_16 => self.read_u16() as usize,
            STRING_32 => self.read_u32() as usize,
            _ => panic!("Invalid string length marker: {}", marker),
        }
    }

    fn read_u8(&mut self) -> i32 {
        let value = self.bytes[self.index];
        self.index += 1;
        return (value & 0xFF).into();
    }

    fn read_u16(&mut self) -> i32 {
        let value = u16::from_be_bytes(self.bytes[self.index..self.index + 2].try_into().unwrap());
        self.index += 2;
        return (value & 0xFFFF).into();
    }

    fn read_u32(&mut self) -> i64 {
        let value = u32::from_be_bytes(self.bytes[self.index..self.index + 4].try_into().unwrap());
        self.index += 4;
        return (value & 0xFFFFFFFF).into();
    }

    fn next_i16(&mut self) -> i16 {
        let value = i16::from_be_bytes(self.bytes[self.index..self.index + 2].try_into().unwrap());
        self.index += 2;
        return value;
    }

    fn next_i32(&mut self) -> i32 {
        let value = i32::from_be_bytes(self.bytes[self.index..self.index + 4].try_into().unwrap());
        self.index += 4;
        return value;
    }

    fn next_i64(&mut self) -> i64 {
        let value = i64::from_be_bytes(self.bytes[self.index..self.index + 8].try_into().unwrap());
        self.index += 8;
        return value;
    }

    fn read_double(&mut self) -> f64 {
        let value = f64::from_be_bytes(self.bytes[self.index..self.index + 8].try_into().unwrap());
        self.index += 8;
        return value;
    }

    fn next_i8(&mut self) -> i8 {
        let value = i8::try_from(self.bytes[self.index]).unwrap();
        self.index += 1;
        return value;
    }
}
