use pyo3::prelude::*;
use pyo3::types::{PyDict, IntoPyDict};

const NULL: u8 = 0xC0;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const FLOAT_64: u8 = 0xC1;
const INT_8: u8 = 0xC8;
const INT_16: u8 = 0xC9;
const INT_32: u8 = 0xCA;
const INT_64: u8 = 0xCB;
const TINY_STRING: u8 = 0x80;
const STRING_8: u8 = 0xD0;
const STRING_16: u8 = 0xD1;
const STRING_32: u8 = 0xD2;
const TINY_LIST: u8 = 0x90;
const LIST_8: u8 = 0xD4;
const LIST_16: u8 = 0xD5;
const LIST_32: u8 = 0xD6;
const TINY_MAP: u8 = 0xA0;
const MAP_8: u8 = 0xD8;
const MAP_16: u8 = 0xD9;
const MAP_32: u8 = 0xDA;
const TINY_STRUCT: u8 = 0xB0;
const STRUCT_8: u8 = 0xDC;
const STRUCT_16: u8 = 0xDD;

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
        let marker = self.bytes[self.index];
        self.index += 1;
        return self.read_value(marker);
    }

    fn read_value(&mut self, marker: u8) -> PyObject {
        let high_nibble = (marker & 0xF0).u8;
        
        match high_nibble {
            TINY_STRING => self.read_string((marker & 0x0F) as usize),
            TINY_LIST => self.read_list((marker & 0x0F) as usize),
            TINY_MAP => self.read_map((marker & 0x0F) as usize),
            _ => match marker {

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
        let data = self.bytes[self.index..self.index + length];
        self.index += length;
        return String::from_utf8(data).unwrap().to_object(self.py);
    }

    fn read_map(&mut self, length: usize) -> PyObject {
        if length == 0 {
            return PyDict::new(self.py).to_object(self.py);
        }

        let mut kvps: Vec<(String, PyObject)> = Vec::with_capacity(length);
        for _ in 0..length {
            let key = self.read_string(self.read_string_length());
            let value = self.read();
            kvps.push(key, value);
        }
        return kvps.into_py_dict(self.py);
    }

    fn read_string_length(&mut self) -> usize {
        let marker = self.bytes[self.index];
        self.index += 1;
        let high_nibble = (marker & 0xF0).u8;
        match high_nibble {
            TINY_STRING => (marker & 0x0F) as usize,
            STRING_8 => self.read_u8() as usize,
            STRING_16 => self.read_u16() as usize,
            STRING_32 => self.read_u32() as usize,
            _ => panic!("Invalid string length marker: {}", marker)
        }
    }

    fn read_u8(&mut self) -> i32 {
        let value = self.bytes[self.index];
        self.index += 1;
        return value & 0xFF;
    }

    fn read_u16(&mut self) -> i32 {
        let value = u16::from_be_bytes(self.bytes[self.index..self.index + 2usize   ]);
        self.index += 2;
        return value & 0xFFFF;
    }

    fn read_u32(&mut self) -> i64 {
        let value = u32::from_be_bytes(self.bytes[self.index..self.index + 4usize]);
        self.index += 4;
        return value & 0xFFFFFFFFu64;
    }

    fn read_u64(&mut self) -> u64 {
        let value = u64::from_be_bytes(self.bytes[self.index..self.index + 8usize]);
        self.index += 8;
        return value;
    }

    fn read_double(&mut self) -> f64 {
        let value = f64::from_be_bytes(self.bytes[self.index..self.index + 8usize]);
        self.index += 8;
        return value;
    }
}
