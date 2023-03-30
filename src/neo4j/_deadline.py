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


from contextlib import contextmanager
from time import perf_counter


class Deadline:
    def __init__(self, timeout):
        if timeout is None or timeout == float("inf"):
            self._deadline = float("inf")
        else:
            self._deadline = perf_counter() + timeout
        self._original_timeout = timeout

    @property
    def original_timeout(self):
        return self._original_timeout

    def expired(self):
        return self.to_timeout() == 0

    def to_timeout(self):
        if self._deadline == float("inf"):
            return None
        timeout = self._deadline - perf_counter()
        return timeout if timeout > 0 else 0

    def __eq__(self, other):
        if isinstance(other, Deadline):
            return self._deadline == other._deadline
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, Deadline):
            return self._deadline > other._deadline
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, Deadline):
            return self._deadline >= other._deadline
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Deadline):
            return self._deadline < other._deadline
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, Deadline):
            return self._deadline <= other._deadline
        return NotImplemented

    @classmethod
    def from_timeout_or_deadline(cls, timeout):
        if isinstance(timeout, cls):
            return timeout
        return cls(timeout)

    def __str__(self):
        return f"Deadline(timeout={self._original_timeout})"


merge_deadlines = min


def merge_deadlines_and_timeouts(*deadline):
    deadlines = map(Deadline.from_timeout_or_deadline, deadline)
    return merge_deadlines(deadlines)


@contextmanager
def connection_deadline(connection, deadline):
    original_deadline = connection.socket.get_deadline()
    if deadline is None and original_deadline is not None:
        # nothing to do here
        yield
        return
    deadline = merge_deadlines(
        (d for d in (deadline, original_deadline) if d is not None)
    )
    connection.socket.set_deadline(deadline)
    try:
        yield
    finally:
        connection.socket.set_deadline(original_deadline)
