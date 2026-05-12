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
    _n_timed: int
    _warmup: int
    _start_time: int | None
    _runs: int
    _timings: list[_Timing]

    def __init__(self, n_timed: int, n_warmup: int) -> None:
        self._n_timed = n_timed
        self._warmup = n_warmup
        self._start_time = None
        self._runs = 0
        self._timings = []

    def __enter__(self) -> None:
        if self._start_time is not None:
            raise RuntimeError("Timer is already running")
        self._start_time = time.time_ns()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is not None:
            # Don't record timings if an exception was raised
            self._start_time = None
            return
        end_time = time.time_ns()
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
        """
        Write the measurements to the CSV file.

        The CSV has 3 columns as expected by the benchmarking framework:

        * ``scheduled_at`` epoch ms: the instant of the scheduled measurement
        * ``started_at`` epoch ms: the instant of the measurement start time,
        * ``stopped_at`` epoch ms: the instant of the measurement finish time.

        Since this benchmark implementation does not really have the notion of
        workload scheduling, the ``scheduled_at`` column is currently always
        filled with the same value as the ``started_at`` column.
        """
        if self._n_timed != len(self._timings):
            raise RuntimeError(
                f"Expected {self._n_timed} timings, "
                f"but got {len(self._timings)}"
            )
        with file_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for timing in self._timings:
                start = timing.format_start()
                end = timing.format_end()
                writer.writerow([start, start, end])


@dataclass(frozen=True)
class _Timing:
    start_ns: int
    end_ns: int

    def format_start(self) -> str:
        ms, ns = divmod(self.start_ns, 1_000_000)
        return f"{ms}.{ns:06d}"

    def format_end(self) -> str:
        ms, ns = divmod(self.end_ns, 1_000_000)
        return f"{ms}.{ns:06d}"
