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


"""Internal module for all internal exception classes."""

from os import strerror
from ssl import SSLError


class BoltError(Exception):
    """Base class for all Bolt protocol errors."""

    def __init__(self, message, address):
        super().__init__(message)
        self.address = address


class BoltConnectionError(BoltError):
    """Raised when a connection fails."""

    def __init__(self, message, address):
        msg = (
            "Connection Failed. "
            "Please ensure that your database is listening on the correct "
            "host and port and that you have enabled encryption if required. "
            "Note that the default encryption setting has changed in Neo4j "
            f"4.0. See the docs for more information. {message}"
        )

        super().__init__(msg, address)

    def __str__(self):
        s = super().__str__()
        errno = self.errno
        if errno:
            s += f" (code {errno}: {strerror(errno)})"
        return s

    @property
    def errno(self):
        if isinstance(self.__cause__, SSLError):
            return None
        try:
            return self.__cause__.errno
        except AttributeError:
            return None


class BoltSecurityError(BoltConnectionError):
    """Raised when a connection fails for security reasons."""

    def __init__(self, message, address):
        super(BoltConnectionError, self).__init__(message, address)

    def __str__(self):
        cause = self.__cause__
        if cause is not None:
            cause_name = cause.__class__.__name__
            cause_msg = str(cause)
            if cause_msg:
                return f"[{cause_name}] {super().__str__()} ({cause_msg})"
            return f"[{cause_name}] {super().__str__()}"
        return super().__str__()

    @property
    def errno(self):
        return None


class BoltHandshakeError(BoltError):
    """Raised when a handshake completes unsuccessfully."""

    def __init__(self, message, address, request_data, response_data):
        super().__init__(message, address)
        self.request_data = request_data
        self.response_data = response_data


class BoltProtocolError(BoltError):
    """Raised when an unexpected or unsupported protocol event occurs."""


class SocketDeadlineExceededError(RuntimeError):
    """Raised from sockets with deadlines when a timeout occurs."""


class QueryApiHttpError(Exception):
    """
    Raised when a protocol error over Query API/HTTP occurs.

    Encountering this error is likely a bug in the driver or the server.
    Please report the issue and attach driver logs if possible.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"protocol error: {message}")
