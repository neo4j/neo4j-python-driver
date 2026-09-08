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

import mock
import pytest

from neo4j import _typing as t
from neo4j._optional_deps import (
    np,
    pa,
    pa_compute,
    pd,
)


if t.TYPE_CHECKING:
    import types

    import aiohttp
    import urllib3
else:
    aiohttp: t.Any = None
    with suppress(ImportError):
        import aiohttp  # type: ignore[no-redef]
    urllib3: t.Any = None
    with suppress(ImportError):
        import urllib3  # type: ignore[no-redef]

    if (aiohttp is None) ^ (urllib3 is None):
        raise ImportError(
            "aiohttp and urllib3 must both be installed or both be missing"
        )


if t.TYPE_CHECKING:
    T_Callable = t.TypeVar("T_Callable", bound=t.Callable)


class _OptionalDepsMock(mock.MagicMock):
    __optional_dep_name: str = "optional dependency"

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        name = kwargs.get("name")
        super().__init__(*args, **kwargs)
        if isinstance(name, str):
            self.__optional_dep_name = name

    def _get_child_mock(self, **kwargs) -> t.Self:
        child = type(self)(**kwargs)
        child.__optional_dep_name = self.__optional_dep_name
        return child


HAS_NP = np is not None
HAS_PD = pd is not None
HAS_PA = pa is not None
HAS_HTTP = aiohttp is not None


if np is None:
    np = _OptionalDepsMock(name="numpy")
if pd is None:
    pd = _OptionalDepsMock(name="pandas")
if pa is None:
    pa = _OptionalDepsMock(name="pyarrow")
    pa_compute = _OptionalDepsMock(name="pyarrow.compute")


__all__ = [
    "HAS_NP",
    "HAS_PA",
    "HAS_PD",
    "mark_skip_if_http_dependencies_missing",
    "mark_skip_with_optional_dependency",
    "mark_skip_without_optional_dependency",
    "np",
    "pa",
    "pa_compute",
    "pd",
    "skip_if_http_dependencies_missing",
    "skip_if_mocked_dependency",
    "skip_if_unsupported_uri",
]

_DEP_NAME = {
    "np": "numpy",
    "pa": "pyarrow",
    "pd": "pandas",
}


def mark_skip_with_optional_dependency(
    optional_dep: types.ModuleType | _OptionalDepsMock,
) -> t.Callable[[T_Callable], T_Callable]:
    if isinstance(optional_dep, _OptionalDepsMock):
        name = optional_dep._OptionalDepsMock__optional_dep_name
    else:
        name = optional_dep.__name__
    return pytest.mark.skipif(
        not isinstance(optional_dep, mock.Mock),
        reason=f"{name} installed",
    )


def mark_skip_without_optional_dependency(
    optional_dep: types.ModuleType | _OptionalDepsMock,
) -> t.Callable[[T_Callable], T_Callable]:
    if isinstance(optional_dep, _OptionalDepsMock):
        name = optional_dep._OptionalDepsMock__optional_dep_name
    else:
        name = optional_dep.__name__
    return pytest.mark.skipif(
        isinstance(optional_dep, mock.Mock),
        reason=f"{name} not installed",
    )


def skip_if_mocked_dependency(dep: t.Any) -> None:
    if isinstance(dep, mock.Mock):
        name_any = getattr(dep, "_OptionalDepsMock__optional_dep_name", None)
        name = name_any if isinstance(name_any, str) else "optional dependency"
        pytest.skip(f"{name} not installed")


mark_skip_if_http_dependencies_missing = pytest.mark.skipif(
    not HAS_HTTP, reason="aiohttp and urllib3 are not installed"
)


def skip_if_http_dependencies_missing() -> None:
    if not HAS_HTTP:
        pytest.skip("aiohttp and urllib3 are not installed")


def skip_if_unsupported_uri(uri: str) -> None:
    if uri.startswith(("http://", "https://")):
        skip_if_http_dependencies_missing()
