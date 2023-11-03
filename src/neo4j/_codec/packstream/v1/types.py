# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


import typing as t

from ...._optional_deps import (
    np,
    pd,
)


NONE_VALUES: t.Tuple = (None,)
TRUE_VALUES: t.Tuple = (True,)
FALSE_VALUES: t.Tuple = (False,)
INT_TYPES: t.Tuple[t.Type, ...] = (int,)
FLOAT_TYPES: t.Tuple[t.Type, ...] = (float,)
# we can't put tuple here because spatial types subclass tuple,
# and we don't want to treat them as sequences
SEQUENCE_TYPES: t.Tuple[t.Type, ...] = (list,)
MAPPING_TYPES: t.Tuple[t.Type, ...] = (dict,)
BYTES_TYPES: t.Tuple[t.Type, ...] = (bytes, bytearray)


if np is not None:
    TRUE_VALUES = (*TRUE_VALUES, np.bool_(True))
    FALSE_VALUES = (*FALSE_VALUES, np.bool_(False))
    INT_TYPES = (*INT_TYPES, np.integer)
    FLOAT_TYPES = (*FLOAT_TYPES, np.floating)
    SEQUENCE_TYPES = (*SEQUENCE_TYPES, np.ndarray)

if pd is not None:
    NONE_VALUES = (*NONE_VALUES, pd.NA)
    SEQUENCE_TYPES = (*SEQUENCE_TYPES, pd.Series, pd.Categorical,
                      pd.core.arrays.ExtensionArray)
    MAPPING_TYPES = (*MAPPING_TYPES, pd.DataFrame)


__all__ = [
    "NONE_VALUES",
    "TRUE_VALUES",
    "FALSE_VALUES",
    "INT_TYPES",
    "FLOAT_TYPES",
    "SEQUENCE_TYPES",
    "MAPPING_TYPES",
    "BYTES_TYPES",
]
