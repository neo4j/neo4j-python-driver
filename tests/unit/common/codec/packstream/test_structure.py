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

from neo4j._codec.packstream import Structure


@pytest.mark.parametrize(
    "args",
    (
        (b"T", 1, 2, 3, "abc", 1.2, None, False),
        (b"F",),
    ),
)
def test_structure_accessors(args):
    tag = args[0]
    fields = list(args[1:])
    s1 = Structure(*args)
    assert s1.tag == tag
    assert s1.fields == fields


@pytest.mark.parametrize(
    ("other", "expected"),
    (
        (Structure(b"T", 1, 2, 3, "abc", 1.2, [{"a": "b"}, None]), True),
        (Structure(b"T", 1, 2, 3, "abc", 1.2, [{"a": "b"}, 0]), False),
        (Structure(b"T", 1, 2, 3, "abc", 1.2, [{"a": "B"}, None]), False),
        (Structure(b"T", 1, 2, 3, "abc", 1.2, [{"A": "b"}, None]), False),
        (Structure(b"T", 1, 2, 3, "abc", 1.3, [{"a": "b"}, None]), False),
        (
            Structure(b"T", 1, 2, 3, "aBc", float("Nan"), [{"a": "b"}, None]),
            False,
        ),
        (Structure(b"T", 2, 2, 3, "abc", 1.2, [{"a": "b"}, None]), False),
        (Structure(b"T", 2, 3, "abc", 1.2, [{"a": "b"}, None]), False),
        (Structure(b"T", [1, 2, 3, "abc", 1.2, [{"a": "b"}, None]]), False),
        (object(), NotImplemented),
    ),
)
def test_structure_equality(other, expected):
    s1 = Structure(b"T", 1, 2, 3, "abc", 1.2, [{"a": "b"}, None])
    assert s1.__eq__(other) is expected  # noqa: PLC2801
    if expected is NotImplemented:
        assert s1.__ne__(other) is NotImplemented  # noqa: PLC2801
    else:
        assert s1.__ne__(other) is not expected  # noqa: PLC2801


@pytest.mark.parametrize(
    ("args", "expected"),
    (
        ((b"F", 1, 2), "Structure(b'F', 1, 2)"),
        ((b"f", [1, 2]), "Structure(b'f', [1, 2])"),
        (
            (b"T", 1.3, None, {"a": "b"}),
            "Structure(b'T', 1.3, None, {'a': 'b'})",
        ),
    ),
)
def test_structure_repr(args, expected):
    s1 = Structure(*args)
    assert repr(s1) == expected
    assert str(s1) == expected

    # Ensure that the repr is consistent with the constructor
    assert eval(repr(s1)) == s1
    assert eval(str(s1)) == s1


@pytest.mark.parametrize(
    ("fields", "expected"),
    (
        ((), 0),
        (([],), 1),
        ((1, 2), 2),
        ((1, 2, []), 3),
        (([1, 2], {"a": "foo", "b": "bar"}), 2),
    ),
)
def test_structure_len(fields, expected):
    structure = Structure(b"F", *fields)
    assert len(structure) == expected


def test_structure_getitem():
    fields = [1, 2, 3, "abc", 1.2, None, False, {"a": "b"}]
    structure = Structure(b"F", *fields)
    for i, field in enumerate(fields):
        assert structure[i] == field
        assert structure[-len(fields) + i] == field
    with pytest.raises(IndexError):
        _ = structure[len(fields)]
    with pytest.raises(IndexError):
        _ = structure[-len(fields) - 1]


def test_structure_setitem():
    test_value = object()
    fields = [1, 2, 3, "abc", 1.2, None, False, {"a": "b"}]
    structure = Structure(b"F", *fields)
    for i, original_value in enumerate(fields):
        structure[i] = test_value
        assert structure[i] == test_value
        assert structure[-len(fields) + i] == test_value
        assert structure[i] != original_value
        assert structure[-len(fields) + i] != original_value

        structure[i] = original_value
        assert structure[i] == original_value
        assert structure[-len(fields) + i] == original_value

        structure[-len(fields) + i] = test_value
        assert structure[i] == test_value
        assert structure[-len(fields) + i] == test_value
        assert structure[i] != original_value
        assert structure[-len(fields) + i] != original_value

        structure[-len(fields) + i] = original_value
        assert structure[i] == original_value
        assert structure[-len(fields) + i] == original_value
    with pytest.raises(IndexError):
        structure[len(fields)] = test_value
    with pytest.raises(IndexError):
        structure[-len(fields) - 1] = test_value
