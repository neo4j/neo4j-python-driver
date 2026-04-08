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


import inspect
from contextlib import suppress

import pytest

from neo4j import ServerInfo
from neo4j._async.io import AsyncBolt
from neo4j._deadline import Deadline
from neo4j.auth_management import AsyncAuthManager
from neo4j.exceptions import Neo4jError


__all__ = [
    "async_fake_connection",
    "async_fake_connection_generator",
    "async_scripted_connection",
    "async_scripted_connection_generator",
]


@pytest.fixture
def async_fake_connection_generator(session_mocker):
    mock = session_mocker.mock_module

    class AsyncFakeConnection(mock.NonCallableMagicMock):
        callbacks: list
        server_info = ServerInfo("127.0.0.1", (4, 3))
        local_port = 1234

        def __init__(self, *args, **kwargs):
            kwargs["spec"] = AsyncBolt
            super().__init__(*args, **kwargs)
            self.attach_mock(mock.Mock(return_value=True), "is_reset_mock")
            self.attach_mock(mock.Mock(return_value=False), "defunct")
            self.attach_mock(mock.Mock(return_value=False), "stale")
            self.attach_mock(mock.Mock(return_value=False), "closed")
            self.attach_mock(mock.Mock(return_value=False), "socket")
            self.attach_mock(mock.Mock(return_value=False), "re_auth")
            self.attach_mock(
                mock.AsyncMock(spec=AsyncAuthManager), "auth_manager"
            )
            self.attach_mock(mock.AsyncMock(), "liveness_check")
            self.unresolved_address = next(iter(args), "localhost")

            self.callbacks = []

            def close_side_effect():
                self.closed.return_value = True

            self.attach_mock(
                mock.AsyncMock(side_effect=close_side_effect), "close"
            )

            for op in ("read", "write"):
                self.socket.attach_mock(
                    mock.Mock(return_value=None), f"get_{op}_deadline"
                )

                def make_set_deadline_side_effect(op_):
                    def side_effect(deadline):
                        deadline = Deadline.from_timeout_or_deadline(deadline)
                        get_mock = getattr(self.socket, f"get_{op_}_deadline")
                        get_mock.return_value = deadline

                    return side_effect

                self.socket.attach_mock(
                    mock.Mock(side_effect=make_set_deadline_side_effect(op)),
                    f"set_{op}_deadline",
                )

        @property
        def is_reset(self):
            if self.closed.return_value or self.defunct.return_value:
                raise AssertionError(
                    "is_reset should not be called on a closed or defunct "
                    "connection."
                )
            return self.is_reset_mock()

        async def fetch_message(self, *args, **kwargs):
            if self.callbacks:
                cb = self.callbacks.pop(0)
                await cb()
            return await super().__getattr__("fetch_message")(*args, **kwargs)

        async def fetch_all(self, *args, **kwargs):
            while self.callbacks:
                cb = self.callbacks.pop(0)
                await cb()
            return await super().__getattr__("fetch_all")(*args, **kwargs)

        def __getattr__(self, name):
            parent = super()

            def build_message_handler(name):
                def func(*args, **kwargs):
                    async def callback():
                        for cb_name, param_count in (
                            ("on_success", 1),
                            ("on_summary", 0),
                        ):
                            cb = kwargs.get(cb_name)
                            if callable(cb):
                                # fails for example for built-in method as cb
                                with suppress(ValueError):
                                    param_count = len(
                                        inspect.signature(cb).parameters
                                    )

                                res = cb({}) if param_count == 1 else cb()
                                # suppress in case the callback is not async
                                with suppress(TypeError):
                                    await res

                    self.callbacks.append(callback)

                return func

            method_mock = parent.__getattr__(name)
            if name in {
                "begin",
                "run",
                "commit",
                "pull",
                "rollback",
                "discard",
                "telemetry",
            }:
                method_mock.side_effect = build_message_handler(name)
            return method_mock

    return AsyncFakeConnection


@pytest.fixture
def async_fake_connection(async_fake_connection_generator):
    return async_fake_connection_generator()


@pytest.fixture
def async_scripted_connection_generator(async_fake_connection_generator):
    class AsyncScriptedConnection(async_fake_connection_generator):
        _script: list
        _script_pos: int
        _telemetry_matching_enabled: bool = False

        def set_script(self, callbacks):
            """
            Set a scripted sequence of callbacks.

            :param callbacks: The callbacks. They should be a list of 2-tuples.
                ``("name_of_message", {"callback_name": arguments})``. E.g., ::

                    [
                        ("run", {"on_success": ({},), "on_summary": None}),
                        ("pull", {
                            "on_records": ([some_record],),
                            "on_success": None,
                            "on_summary": None,
                        })
                        # use any exception to throw it instead of calling handlers
                        ("commit", RuntimeError("oh no!"))
                    ]

                Note that arguments can be :data:`None`. In this case,
                ScriptedConnection will make a guess on best-suited default
                arguments.
            """  # noqa: E501 example code isn't too long
            self._script = callbacks
            if any(name for name, _ in callbacks if name == "telemetry"):
                self._telemetry_matching_enabled = True
            self._script_pos = 0

        def enable_telemetry_matching(self, value=True):
            self._telemetry_matching_enabled = value

        def __getattr__(self, name):
            parent = super()

            def build_message_handler(name):
                def func(*args, **kwargs):
                    try:
                        expected_message, scripted_callbacks = self._script[
                            self._script_pos
                        ]
                    except IndexError:
                        pytest.fail("End of scripted connection reached.")
                    assert name == expected_message
                    self._script_pos += 1

                    async def callback():
                        if isinstance(scripted_callbacks, BaseException):
                            raise scripted_callbacks
                        error = None
                        for cb_name, default_cb_args in (
                            ("on_ignored", ({},)),
                            ("on_failure", ({},)),
                            ("on_records", ([],)),
                            ("on_success", ({},)),
                            ("on_summary", ()),
                        ):
                            cb = kwargs.get(cb_name)
                            if cb_name not in scripted_callbacks:
                                continue
                            cb_args = scripted_callbacks[cb_name]
                            if cb_args is None:
                                cb_args = default_cb_args
                            if cb_name == "on_failure":
                                error = Neo4jError._hydrate_gql(**cb_args[0])
                            if not callable(cb):
                                continue
                            res = cb(*cb_args)
                            # suppress in case the callback is not async
                            with suppress(TypeError):
                                await res
                        if error is not None:
                            raise error

                    self.callbacks.append(callback)

                return func

            method_mock = parent.__getattr__(name)
            if name in {
                "begin",
                "run",
                "commit",
                "pull",
                "rollback",
                "discard",
            }:
                method_mock.side_effect = build_message_handler(name)
            if name == "telemetry" and self._telemetry_matching_enabled:
                method_mock.side_effect = build_message_handler(name)
            return method_mock

    return AsyncScriptedConnection


@pytest.fixture
def async_scripted_connection(async_scripted_connection_generator):
    return async_scripted_connection_generator()
