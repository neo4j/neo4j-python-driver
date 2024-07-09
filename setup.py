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

# TODO: 6.0 - the whole deprecation and double naming shebang can be removed


import pathlib
import sys
import warnings

from setuptools import setup
from setuptools_rust import RustExtension


sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))

from neo4j._meta import (
    deprecated_package as deprecated,
    native_extensions_build,
    native_extensions_force,
    package,
)


if deprecated:
    warnings.warn(
        f"`{package}` is deprecated, please install `neo4j` instead.",
        DeprecationWarning
    )


rust_extensions = []

if native_extensions_build:
    rust_extensions.append(
        RustExtension(
            "neo4j._codec.packstream._rust",
            path="rust_ext/Cargo.toml",
            optional=not native_extensions_force,
        )
    )


setup(rust_extensions=rust_extensions)
