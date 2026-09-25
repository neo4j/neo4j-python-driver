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


import pytest

from neo4j.types import UnsupportedType


def test_construction():
    value = UnsupportedType._new("UUID", (255, 128), None)

    assert isinstance(value, UnsupportedType)
    assert value.name == "UUID"
    assert value.minimum_protocol_version == (255, 128)
    assert value.message is None


def test_construction_with_message():
    value = UnsupportedType._new("UUID", (255, 128), "Needs some config...")

    assert isinstance(value, UnsupportedType)
    assert value.name == "UUID"
    assert value.minimum_protocol_version == (255, 128)
    assert value.message == "Needs some config..."


@pytest.mark.parametrize("name", ("FluxCompensationFactor", "", "Type"))
@pytest.mark.parametrize("message", (None, "", "Some cool text"))
def test_str(name, message):
    value = UnsupportedType._new(name, (1, 2), message)
    assert str(value) == f"UnsupportedType<{name}>"


@pytest.mark.parametrize("name", ("EncryptedValue", "", "Type"))
@pytest.mark.parametrize("version", ((0, 0), (1, 2), (255, 128), (128, 255)))
@pytest.mark.parametrize("message", (None, "", "Some cool text"))
def test_repr(name, version, message):
    if message is not None:
        expected_repr = (
            "<UnsupportedType "
            f"name={name!r} "
            f"minimum_protocol_version={version} "
            f"message={message!r}>"
        )
    else:
        expected_repr = (
            "<UnsupportedType "
            f"name={name!r} "
            f"minimum_protocol_version={version}>"
        )

    value = UnsupportedType._new(name, version, message)

    assert repr(value) == expected_repr
