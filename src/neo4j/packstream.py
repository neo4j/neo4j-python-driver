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


# TODO: 6.0 - remove this file


from ._codec.packstream.v1 import (
    INT64_MAX,
    INT64_MIN,
    PACKED_UINT_8,
    PACKED_UINT_16,
    Packer,
    Structure,
    UnpackableBuffer,
    UNPACKED_UINT_8,
    UNPACKED_UINT_16,
    Unpacker,
)
from ._meta import deprecation_warn


__all__ = [
    "PACKED_UINT_8",
    "PACKED_UINT_16",
    "UNPACKED_UINT_8",
    "UNPACKED_UINT_16",
    "INT64_MIN",
    "INT64_MAX",
    "Structure",
    "Packer",
    "Unpacker",
    "UnpackableBuffer",
]

deprecation_warn(
    "The module `neo4j.packstream` was made internal and will "
    "no longer be available for import in future versions.",
    stack_level=2
)
