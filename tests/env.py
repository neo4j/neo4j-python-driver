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


import abc
import sys
import types
import typing as t
from os import environ


class _LazyEval(abc.ABC):
    @abc.abstractmethod
    def eval(self):
        pass


class _LazyEvalEnv(_LazyEval):
    def __init__(self, env_key, type_: t.Type = str, default=...):
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
                raise Exception(
                    f"Missing environment variable {self.env_key}"
                ) from e
        if self.type_ is bool:
            return value.lower() in ("yes", "y", "1", "on", "true")
        if self.type_ is not None:
            return self.type_(value)


class _LazyEvalFunc(_LazyEval):
    def __init__(self, func):
        self.func = func

    def eval(self):
        return self.func()


class _Module:
    def __init__(self, module):
        self._module = module

    def __getattr__(self, item):
        val = getattr(self._module, item)
        if isinstance(val, _LazyEval):
            val = val.eval()
            setattr(self._module, item, val)
        return val


_module = _Module(sys.modules[__name__])

sys.modules[__name__] = _module  # type: ignore[assignment]


NEO4J_HOST = _LazyEvalEnv("TEST_NEO4J_HOST")
NEO4J_PORT = _LazyEvalEnv("TEST_NEO4J_PORT", int)
NEO4J_USER = _LazyEvalEnv("TEST_NEO4J_USER")
NEO4J_PASS = _LazyEvalEnv("TEST_NEO4J_PASS")
NEO4J_SCHEME = _LazyEvalEnv("TEST_NEO4J_SCHEME")
NEO4J_EDITION = _LazyEvalEnv("TEST_NEO4J_EDITION")
NEO4J_VERSION = _LazyEvalEnv("TEST_NEO4J_VERSION")
NEO4J_IS_CLUSTER = _LazyEvalEnv("TEST_NEO4J_IS_CLUSTER", bool)
NEO4J_SERVER_URI = _LazyEvalFunc(
    lambda: f"{_module.NEO4J_SCHEME}://{_module.NEO4J_HOST}:"
            f"{_module.NEO4J_PORT}"
)


__all__ = (
    "NEO4J_HOST",
    "NEO4J_PORT",
    "NEO4J_USER",
    "NEO4J_PASS",
    "NEO4J_SCHEME",
    "NEO4J_EDITION",
    "NEO4J_VERSION",
    "NEO4J_IS_CLUSTER",
    "NEO4J_SERVER_URI",
)
