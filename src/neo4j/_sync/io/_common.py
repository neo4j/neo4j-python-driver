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


import asyncio
import inspect
import logging

from ..._async_compat.util import Util
from ...exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
)


log = logging.getLogger("neo4j.io")


class ConnectionErrorHandler:
    """
    Wrapper class for handling connection errors.

    The class will wrap each method to invoke a callback if the method raises
    Neo4jError, SessionExpired, or ServiceUnavailable.
    The error will be re-raised after the callback.

    :param connection the connection object to warp
    :type connection Bolt
    :param on_error the function to be called when a method of
        connection raises of the caught errors.
    :type on_error callable
    """

    def __init__(self, connection, on_error):
        self.__connection = connection
        self.__on_error = on_error

    def __getattr__(self, name):
        connection_attr = getattr(self.__connection, name)
        if not callable(connection_attr):
            return connection_attr

        def outer(func):
            def inner(*args, **kwargs):
                try:
                    func(*args, **kwargs)
                except (Neo4jError, ServiceUnavailable, SessionExpired) as exc:
                    assert not inspect.iscoroutinefunction(self.__on_error)
                    self.__on_error(exc)
                    raise

            return inner

        def outer_async(coroutine_func):
            def inner(*args, **kwargs):
                try:
                    coroutine_func(*args, **kwargs)
                except (
                    Neo4jError,
                    ServiceUnavailable,
                    SessionExpired,
                    asyncio.CancelledError,
                ) as exc:
                    Util.callback(self.__on_error, exc)
                    raise

            return inner

        if inspect.iscoroutinefunction(connection_attr):
            return outer_async(connection_attr)
        return outer(connection_attr)

    def __setattr__(self, name, value):
        if name.startswith("_" + self.__class__.__name__ + "__"):
            super().__setattr__(name, value)
        else:
            setattr(self.__connection, name, value)
