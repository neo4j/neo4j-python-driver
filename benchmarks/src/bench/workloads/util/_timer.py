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

import csv
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


class Timer:
    _n: int
    _warmup: int
    _start_time: float | None
    _runs: int
    _timings: list[_Timing]

    def __init__(self, n: int, warmup: int) -> None:
        self._n = n
        self._warmup = warmup
        self._start_time = None
        self._runs = 0
        self._timings = []

    def __enter__(self) -> None:
        if self._start_time is not None:
            raise RuntimeError("Timer is already running")
        self._start_time = time.perf_counter()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is not None:
            # Don't record timings if an exception was raised
            self._start_time = None
            return
        end_time = time.perf_counter()
        if self._start_time is None:
            raise RuntimeError("Timer is not running")
        if self._runs >= self._warmup:
            self._timings.append(_Timing(self._start_time, end_time))
        self._start_time = None
        self._runs += 1

    def reset(self) -> None:
        if self._start_time is not None:
            raise RuntimeError("Cannot reset timer while it is running")
        self._runs = 0
        self._timings = []

    def flush_csv(self, file_path: Path) -> None:
        if self._n != len(self._timings):
            raise RuntimeError(
                f"Expected {self._n} timings, but got {len(self._timings)}"
            )
        with file_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for timing in self._timings:
                writer.writerow([timing.start, timing.start, timing.end])


@dataclass(frozen=True)
class _Timing:
    start: float
    end: float
