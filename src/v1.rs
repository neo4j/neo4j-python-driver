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

mod pack;
mod unpack;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

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
const BYTES_8: u8 = 0xCC;
const BYTES_16: u8 = 0xCD;
const BYTES_32: u8 = 0xCE;

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(unpack::unpack, m)?)?;
    m.add_function(wrap_pyfunction!(pack::pack, m)?)?;

    Ok(())
}
