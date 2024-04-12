# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import inspect

import pytest

from neo4j import ServerInfo
from neo4j._deadline import Deadline


__all__ = [
    "fake_connection",
    "fake_connection_generator",
]


@pytest.fixture
def fake_connection_generator(session_mocker):
    mock = session_mocker.mock_module

    class FakeConnection(mock.NonCallableMagicMock):
        callbacks = []
        server_info = ServerInfo("127.0.0.1", (4, 3))
        local_port = 1234
        bolt_patches = set()

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.attach_mock(mock.Mock(return_value=True), "is_reset_mock")
            self.attach_mock(mock.Mock(return_value=False), "defunct")
            self.attach_mock(mock.Mock(return_value=False), "stale")
            self.attach_mock(mock.Mock(return_value=False), "closed")
            self.attach_mock(mock.Mock(return_value=False), "socket")
            self.socket.attach_mock(
                mock.Mock(return_value=None), "get_deadline"
            )

            def set_deadline_side_effect(deadline):
                deadline = Deadline.from_timeout_or_deadline(deadline)
                self.socket.get_deadline.return_value = deadline

            self.socket.attach_mock(
                mock.Mock(side_effect=set_deadline_side_effect), "set_deadline"
            )

            def close_side_effect():
                self.closed.return_value = True

            self.attach_mock(mock.Mock(side_effect=close_side_effect), "close")

        @property
        def is_reset(self):
            if self.closed.return_value or self.defunct.return_value:
                raise AssertionError("is_reset should not be called on a closed or "
                                     "defunct connection.")
            return self.is_reset_mock()

        def fetch_message(self, *args, **kwargs):
            if self.callbacks:
                cb = self.callbacks.pop(0)
                cb()
            return super().__getattr__("fetch_message")(*args, **kwargs)

        def fetch_all(self, *args, **kwargs):
            while self.callbacks:
                cb = self.callbacks.pop(0)
                cb()
            return super().__getattr__("fetch_all")(*args, **kwargs)

        def __getattr__(self, name):
            parent = super()

            def build_message_handler(name):
                def func(*args, **kwargs):
                    def callback():
                        for cb_name, param_count in (
                            ("on_success", 1),
                            ("on_summary", 0)
                        ):
                            cb = kwargs.get(cb_name, None)
                            if callable(cb):
                                try:
                                    param_count = \
                                        len(inspect.signature(cb).parameters)
                                except ValueError:
                                    # e.g. built-in method as cb
                                    pass
                                if param_count == 1:
                                    cb({})
                                else:
                                    cb()
                    self.callbacks.append(callback)

                return func

            method_mock = parent.__getattr__(name)
            if name in ("run", "commit", "pull", "rollback", "discard"):
                method_mock.side_effect = build_message_handler(name)
            return method_mock

    return FakeConnection


@pytest.fixture
def fake_connection(fake_connection_generator):
    return fake_connection_generator()
