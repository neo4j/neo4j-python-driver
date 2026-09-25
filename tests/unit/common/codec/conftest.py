# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from contextlib import suppress

import pytest

from ...._optional_deps import (
    HAS_NP,
    HAS_PA,
    HAS_PD,
    np,
    pa,
    pd,
    skip_if_mocked_dependency,
)


@pytest.fixture(params=(True, False))
def np_float_overflow_as_error(request):
    should_raise = request.param
    if should_raise:
        old_err = np.seterr(over="raise")
    else:
        old_err = np.seterr(over="ignore")
    yield
    np.seterr(**old_err)


@pytest.fixture(
    params=(
        int,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.longlong,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.ulonglong,
    ),
    ids=(
        "int",
        "np.int8",
        "np.int16",
        "np.int32",
        "np.int64",
        "np.longlong",
        "np.uint8",
        "np.uint16",
        "np.uint32",
        "np.uint64",
        "np.ulonglong",
    ),
)
def int_type(request):
    skip_if_mocked_dependency(request.param)

    if HAS_NP and issubclass(request.param, np.number):

        def _int_type(value):
            # this avoids deprecation warning from NEP50 and forces
            # c-style wrapping of the value
            return np.array(value).astype(request.param).item()

        return _int_type
    else:
        return request.param


@pytest.fixture(
    params=(float, np.float16, np.float32, np.float64, np.longdouble),
    ids=("float", "np.float16", "np.float32", "np.float64", "np.longdouble"),
)
def float_type(request, np_float_overflow_as_error):
    skip_if_mocked_dependency(request.param)
    return request.param


@pytest.fixture(params=(bool, np.bool_), ids=("bool", "np.bool_"))
def bool_type(request):
    skip_if_mocked_dependency(request.param)
    return request.param


@pytest.fixture(
    params=(bytes, bytearray, np.bytes_),
    ids=("bytes", "bytearray", "np.bytes_"),
)
def bytes_type(request):
    skip_if_mocked_dependency(request.param)
    return request.param


@pytest.fixture(params=(str, np.str_), ids=("str", "np.str_"))
def str_type(request):
    skip_if_mocked_dependency(request.param)
    return request.param


@pytest.fixture(
    params=(
        list,
        tuple,
        np.array,
        pd.Series,
        pd.array,
        pd.arrays.SparseArray,
        pd.arrays.NumpyExtensionArray,
        *((pd.arrays.ArrowExtensionArray,) if HAS_PA else ()),
    ),
    ids=(
        "list",
        "tuple",
        "np.array",
        "pd.Series",
        "pd.array",
        "pd.arrays.SparseArray",
        "pd.arrays.NumpyExtensionArray",
        *(("pd.arrays.ArrowExtensionArray",) if HAS_PA else ()),
    ),
)
def sequence_type(request):
    skip_if_mocked_dependency(request.param)

    if HAS_PD and request.param is pd.Series:
        constructor = _pd_series
    elif HAS_PD and request.param is pd.array:
        constructor = _pd_array
    elif HAS_NP and HAS_PD and request.param is pd.arrays.NumpyExtensionArray:
        constructor = _np_extension_array
    elif HAS_PD and HAS_PA and request.param is pd.arrays.ArrowExtensionArray:
        constructor = pa_extension_array
    elif HAS_NP and request.param is np.array:
        constructor = _np_array
    else:
        constructor = request.param

    return constructor


def _np_array(value):
    with suppress(ValueError):
        return np.array(value)
    return np.array(value, dtype=object)


def _pd_series(value):
    if not value:
        return pd.Series(dtype=object)
    return pd.Series(value)


def _pd_array(value):
    with suppress(ValueError):
        return pd.array(value)
    return pd.array(value, dtype=object)


def _np_extension_array(value):
    array = _np_array(value)
    return pd.arrays.NumpyExtensionArray(array)


def pa_extension_array(value):
    value = tuple(map(_map_pa_extension_array, value))
    array = _pa_array(value)
    return pd.arrays.ArrowExtensionArray(array)


def _map_pa_extension_array(v):
    if isinstance(v, pd.arrays.ArrowExtensionArray):
        v = pa.array(v)
    if isinstance(v, pa.Array):
        v = v.to_pylist()
    return v


def _pa_array(value):
    try:
        return pa.array(value)
    except (pa.ArrowInvalid, ValueError) as exc:
        pytest.skip(f"Value not supported by pyarrow: {exc}")
