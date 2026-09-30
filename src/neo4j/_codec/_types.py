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


from .._optional_deps import (
    np,
    pd,
)


NONE_VALUES: tuple = (None,)
TRUE_VALUES: tuple = (True,)
FALSE_VALUES: tuple = (False,)
INT_TYPES_EXACT: tuple[type, ...] = (int,)
# int types that don't make sense to match with `type(x) is int_type`
# but require `isinstance(x, int_type)`
INT_TYPES_SUB: tuple[type, ...] = ()
FLOAT_TYPES_EXACT: tuple[type, ...] = (float,)
FLOAT_TYPES_SUB: tuple[type, ...] = ()
# we can't put tuple here because spatial types subclass tuple,
# and we don't want to treat them as sequences
SEQUENCE_TYPES: tuple[type, ...] = (list,)
MAPPING_TYPES: tuple[type, ...] = (dict,)
BYTES_TYPES: tuple[type, ...] = (bytes, bytearray)


if np is not None:
    TRUE_VALUES = (*TRUE_VALUES, np.bool_(True))
    FALSE_VALUES = (*FALSE_VALUES, np.bool_(False))
    INT_TYPES_SUB = (*INT_TYPES_SUB, np.integer)
    FLOAT_TYPES_SUB = (*FLOAT_TYPES_SUB, np.floating)
    SEQUENCE_TYPES = (*SEQUENCE_TYPES, np.ndarray)


NON_PD_SEQUENCE_TYPES: tuple[type, ...] = SEQUENCE_TYPES
PD_SEQUENCE_TYPES: tuple[type, ...] = ()
if pd is not None:
    NONE_VALUES = (*NONE_VALUES, pd.NA, pd.NaT)
    PD_SEQUENCE_TYPES = (
        pd.Series,
        pd.Categorical,
        pd.api.extensions.ExtensionArray,
    )
    SEQUENCE_TYPES = (
        *NON_PD_SEQUENCE_TYPES,
        *PD_SEQUENCE_TYPES,
    )
    MAPPING_TYPES = (*MAPPING_TYPES, pd.DataFrame)


INT_TYPES = (*INT_TYPES_EXACT, *INT_TYPES_SUB)
FLOAT_TYPES = (*FLOAT_TYPES_EXACT, *FLOAT_TYPES_SUB)


__all__ = [
    "BYTES_TYPES",
    "FALSE_VALUES",
    "FLOAT_TYPES",
    "FLOAT_TYPES_EXACT",
    "FLOAT_TYPES_SUB",
    "INT_TYPES",
    "INT_TYPES_EXACT",
    "INT_TYPES_SUB",
    "MAPPING_TYPES",
    "NONE_VALUES",
    "NON_PD_SEQUENCE_TYPES",
    "PD_SEQUENCE_TYPES",
    "SEQUENCE_TYPES",
    "TRUE_VALUES",
]
