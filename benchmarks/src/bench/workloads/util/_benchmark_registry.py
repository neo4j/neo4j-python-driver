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

import time
from functools import wraps
from typing import (
    Protocol,
    TYPE_CHECKING,
)

from ._bencher import RetriesExceededError


if TYPE_CHECKING:
    from ._bencher import Bencher


class _TBench(Protocol):
    __name__: str

    def __call__(self, bencher: Bencher) -> None: ...


_REGISTRY: dict[str, _TBench] = {}


def benchmark(func: _TBench) -> _TBench:
    id_ = func.__name__.replace("_", "-")
    if id_ in _REGISTRY:
        raise ValueError(f"Benchmark with id '{id_}' is already registered.")

    @wraps(func)
    def work(bencher: Bencher) -> None:
        attempt_left = 30
        exceptions: list[Exception] = []
        while attempt_left > 0:
            try:
                func(bencher)
            except RetriesExceededError:
                raise
            except Exception as e:
                exceptions.append(e)
            else:
                return
            attempt_left -= 1
            bencher.reset()
            time.sleep(0.5)
        raise ExceptionGroup("Benchmark failed after 30 attempts.", exceptions)

    _REGISTRY[id_] = work
    return work


def get_benchmark(id_: str) -> _TBench:
    try:
        return _REGISTRY[id_]
    except KeyError:
        known = "\n".join(f"'{k}'" for k in sorted(_REGISTRY.keys()))
        raise ValueError(
            f"Unknown benchmark id: '{id_}'\n\n"
            f"Known benchmark ids are:\n{known}"
        ) from None
