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
from typing import TYPE_CHECKING

from ._timer import Timer


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import ParamSpec

    from ._benchmark_context import BenchmarkContext

    P = ParamSpec("P")


class RetriesExceededError(ExceptionGroup):
    pass


class Bencher:
    n_total: int
    n_warmup: int
    timer: Timer
    ctx: BenchmarkContext

    def __init__(
        self,
        n_timed: int,
        n_warmup: int,
        ctx: BenchmarkContext,
    ) -> None:
        self.n_total = n_timed + n_warmup
        self.n_warmup = n_warmup
        self.timer = Timer(n_timed, n_warmup)
        self.ctx = ctx

    def reset(self) -> None:
        self.timer.reset()

    def timed_loop(
        self,
        work: Callable[P, None],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        timed_work = self.timed_work(work)
        for _ in range(self.n_total):
            self.iteration(timed_work, *args, **kwargs)

    def timed_work(self, work: Callable[P, None]) -> Callable[P, None]:
        @wraps(work)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
            with self.timer:
                work(*args, **kwargs)

        return wrapped

    def loop(
        self,
        work: Callable[P, None],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        for _ in range(self.n_total):
            self.iteration(work, *args, **kwargs)

    def iteration(
        self,
        work: Callable[P, None],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        attempts_left = 20
        exceptions: list[Exception] = []
        while attempts_left > 0:
            try:
                work(*args, **kwargs)
            except Exception as e:
                exceptions.append(e)
            else:
                return
            attempts_left -= 1
            time.sleep(1)
        raise RetriesExceededError(
            "Failed to execute work after 20 retries", exceptions
        )
