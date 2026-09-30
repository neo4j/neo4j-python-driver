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


from __future__ import annotations as _

from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Generator,
    Hashable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    Sequence,
    Set,
    Sized,
    ValuesView,
)
from contextlib import AbstractContextManager
from importlib.util import find_spec as _find_spec
from types import EllipsisType
from typing import (
    Any,
    cast,
    ClassVar,
    Concatenate,
    Final,
    Generic,
    Literal,
    NamedTuple,
    overload,
    ParamSpec,
    Protocol,
    SupportsIndex,
    TextIO,
    TYPE_CHECKING,
    TypeAlias,
    TypedDict,
    TypeGuard,
    TypeVar,
    Union,
)


__all__: list[str] = [
    "TYPE_CHECKING",
    "AbstractContextManager",
    "Any",
    "AsyncIterator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "Collection",
    "Concatenate",
    "Coroutine",
    "EllipsisType",
    "Final",
    "Generator",
    "Generic",
    "Hashable",
    "ItemsView",
    "Iterable",
    "Iterator",
    "KeysView",
    "Literal",
    "Mapping",
    "NamedTuple",
    "ParamSpec",
    "Protocol",
    "Sequence",
    "Set",
    "Sized",
    "SupportsIndex",
    "TextIO",
    "TypeAlias",
    "TypeGuard",
    "TypeVar",
    "TypedDict",
    "Union",
    "ValuesView",
    "assert_never",
    "cast",
    "overload",
]


_te_available = _find_spec("typing_extensions") is not None

if TYPE_CHECKING or _te_available:
    from typing_extensions import assert_never  # Python 3.11+
    from typing_extensions import LiteralString  # Python 3.11+
    from typing_extensions import Never  # Python 3.11+
    from typing_extensions import NotRequired  # Python 3.11+
    from typing_extensions import Self  # Python 3.11+
    from typing_extensions import TypeVarTuple  # Python 3.11+
    from typing_extensions import Unpack  # Python 3.11+

    __all__ = [  # noqa: PLE0604 false positive
        *__all__,
        "LiteralString",
        "Never",
        "Self",
        "NotRequired",
        "Unpack",
        "TypeVarTuple",
    ]
else:

    def assert_never(arg: Any, /) -> None:
        value = repr(arg)
        if len(value) > 100:
            value = value[:100] + "..."
        raise AssertionError(
            f"Expected code to be unreachable, but got: {value}"
        )
