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

import abc
import enum
import re
import sys
from os import environ

from neo4j import _typing as t


if t.TYPE_CHECKING:
    from types import ModuleType


_TRUE_ENV_VALUES = {"1", "y", "yes", "true", "t", "on"}


class _LazyEval(abc.ABC):
    @abc.abstractmethod
    def eval(self):
        pass


class _LazyEvalEnv(_LazyEval):
    def __init__(self, env_key, type_: type = str, default: t.Any = ...):
        self.env_key = env_key
        self.type_ = type_
        self.default = default

    def eval(self):
        if self.default is not ...:
            value = environ.get(self.env_key, default=self.default)
        else:
            try:
                value = environ[self.env_key]
            except KeyError as e:
                raise RuntimeError(
                    f"Missing environment variable {self.env_key}"
                ) from e
        if self.type_ is bool:
            return value.lower() in _TRUE_ENV_VALUES
        return self.type_(value)


class _LazyEvalFunc(_LazyEval):
    def __init__(self, func):
        self.func = func

    def eval(self):
        return self.func()


class _Module:
    _module: ModuleType

    def __init__(self, module):
        self._module = module

    def __getattr__(self, item):
        val = getattr(self._module, item)
        if isinstance(val, _LazyEval):
            val = val.eval()
            setattr(self._module, item, val)
        return val


def _get_url() -> str:
    return (
        f"{_module.NEO4J_SCHEME}://{_module.NEO4J_HOST}:{_module.NEO4J_PORT}"
    )


class Scheme(str, enum.Enum):
    BOLT = "bolt"
    NEO4J = "neo4j"
    HTTP = "http"


def _parse_scheme() -> Scheme:
    scheme = _module.NEO4J_SCHEME
    if re.match(r"^bolt(\+s(sc)?)?", scheme):
        return Scheme.BOLT
    if re.match(r"^neo4j(\+s(sc)?)?", scheme):
        return Scheme.NEO4J
    if re.match(r"^http(s)?", scheme):
        return Scheme.HTTP
    raise ValueError(f"Unknown scheme: {scheme!r}")


_module = _Module(sys.modules[__name__])

sys.modules[__name__] = _module  # type: ignore[assignment]


NEO4J_HOST = t.cast(str, _LazyEvalEnv("TEST_NEO4J_HOST"))
NEO4J_PORT = t.cast(int, _LazyEvalEnv("TEST_NEO4J_PORT", int))
NEO4J_USER = t.cast(str, _LazyEvalEnv("TEST_NEO4J_USER"))
NEO4J_PASS = t.cast(str, _LazyEvalEnv("TEST_NEO4J_PASS"))
NEO4J_SCHEME = t.cast(str, _LazyEvalEnv("TEST_NEO4J_SCHEME"))
NEO4J_PARSED_SCHEME = t.cast(Scheme, _LazyEvalFunc(_parse_scheme))
NEO4J_EDITION = t.cast(str, _LazyEvalEnv("TEST_NEO4J_EDITION"))
NEO4J_VERSION = t.cast(str, _LazyEvalEnv("TEST_NEO4J_VERSION"))
NEO4J_IS_CLUSTER = t.cast(bool, _LazyEvalEnv("TEST_NEO4J_IS_CLUSTER", bool))
NEO4J_SERVER_URI = t.cast(str, _LazyEvalFunc(_get_url))
NEO4J_DEFAULT_DB = t.cast(
    str, _LazyEvalEnv("TEST_NEO4J_DEFAULT_DB", default="neo4j")
)
IS_WIN = sys.platform in {"win32", "cygwin"}


__all__ = (
    "IS_WIN",
    "NEO4J_EDITION",
    "NEO4J_HOST",
    "NEO4J_IS_CLUSTER",
    "NEO4J_PARSED_SCHEME",
    "NEO4J_PASS",
    "NEO4J_PORT",
    "NEO4J_SCHEME",
    "NEO4J_SERVER_URI",
    "NEO4J_USER",
    "NEO4J_VERSION",
)
