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

import traceback
import typing as t

import pytest

from neo4j._codec.hydration import BrokenHydrationObject


if t.TYPE_CHECKING:
    from neo4j._codec.hydration.http._common import (
        HydrationHandlerHttpBase,
        HydrationScopeHttp,
    )

    T_Transformer: t.TypeAlias = t.Callable[[t.Any], t.Any]


class HydrationHandlerTestBase:
    @pytest.fixture
    def hydration_handler(self) -> HydrationHandlerHttpBase:
        raise NotImplementedError

    @pytest.fixture
    def hydration_scope(self, hydration_handler) -> HydrationScopeHttp:
        return hydration_handler.new_hydration_scope()

    @pytest.fixture
    def transformer(self, hydration_scope) -> T_Transformer:
        def transformer(value):
            transformer_ = hydration_scope.dehydration_hooks.get_transformer(
                value
            )
            assert callable(transformer_)
            return transformer_(value)

        return transformer

    @staticmethod
    def assert_is_hydrated_type(
        value: object, type_: type | tuple[type, ...]
    ) -> None:
        __tracebackhide__ = True

        if isinstance(value, BrokenHydrationObject):
            traceback.print_exception(value.error)
        if not isinstance(value, type_):
            raise pytest.fail(
                f"Expected value of type {type_}, "
                f"but got {type(value)} instead."
            )
