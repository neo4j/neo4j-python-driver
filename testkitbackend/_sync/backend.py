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
import traceback
from inspect import (
    getmembers,
    isfunction,
)
from json import (
    dumps,
    loads,
)
from pathlib import Path

import neo4j
from neo4j._exceptions import BoltError
from neo4j.exceptions import (
    DriverError,
    Neo4jError,
    UnsupportedServerProduct,
)

from .._driver_logger import (
    buffer_handler,
    log,
)
from ..backend import Request
from ..exceptions import MarkdAsDriverError
from . import requests


TESTKIT_BACKEND_PATH = Path(__file__).absolute().resolve().parents[1]
DRIVER_PATH = Path(neo4j.__path__[0]).absolute().resolve()


def snake_case_to_pascal_case(name: str) -> str:
    return "".join(word.capitalize() for word in name.split("_"))


def get_handler_name(handler):
    name = getattr(handler, "__handler_name__", None)
    if name is not None:
        return name
    return snake_case_to_pascal_case(handler.__name__)


class Backend:
    def __init__(self, rd, wr):
        self._rd = rd
        self._wr = wr
        self.drivers = {}
        self.custom_resolutions = {}
        self.dns_resolutions = {}
        self.auth_token_managers = {}
        self.auth_token_supplies = {}
        self.auth_token_on_expiration_supplies = {}
        self.basic_auth_token_supplies = {}
        self.expiring_auth_token_supplies = {}
        self.client_cert_providers = {}
        self.client_cert_supplies = {}
        self.bookmark_managers = {}
        self.bookmarks_consumptions = {}
        self.bookmarks_supplies = {}
        self.sessions = {}
        self.results = {}
        self.errors = {}
        self.transactions = {}
        self.errors = {}
        self.key = 0
        self.fake_time = None
        self.fake_time_ticker = None
        # Collect all request handlers
        self._requestHandlers = {
            get_handler_name(func): func
            for _, func in getmembers(requests, isfunction)
        }

    def close(self):
        for dict_of_closables in (
            self.transactions,
            {key: tracker.session for key, tracker in self.sessions.items()},
            self.drivers,
        ):
            for key, closable in dict_of_closables.items():
                if not hasattr(closable, "close"):  # e.g., ManagedTransaction
                    continue
                try:
                    closable.close()
                except (Neo4jError, DriverError, OSError):
                    log.error(
                        "Error during TestKit backend garbage collection. "
                        "While collecting: (key: %s) %s\n%s",
                        key,
                        closable,
                        traceback.format_exc(),
                    )
            dict_of_closables.clear()

    def next_key(self):
        self.key += 1
        return self.key

    def process_request(self):
        # Read next request from the stream and processes it.
        in_request = False
        request = ""
        for line in self._rd:
            # Remove trailing newline
            line = line.decode("UTF-8").rstrip()
            if line == "#request begin":
                in_request = True
            elif line == "#request end":
                self._process(request)
                return True
            elif in_request:
                request += line
        return False

    @staticmethod
    def _exc_stems_from_driver(exc):
        stack = traceback.extract_tb(exc.__traceback__)
        for frame in stack[-1:1:-1]:
            p = Path(frame.filename)
            if TESTKIT_BACKEND_PATH in p.parents:
                return False
            if DRIVER_PATH in p.parents:
                return True
        return None

    @staticmethod
    def _exc_msg(exc, max_depth=10):
        if isinstance(exc, Neo4jError) and exc.message is not None:
            return str(exc.message)

        depth = 0
        res = str(exc)
        while getattr(exc, "__cause__", None) is not None:
            depth += 1
            if depth >= max_depth:
                break
            res += f"\nCaused by: {exc.__cause__!r}"
            exc = exc.__cause__
        return res

    def write_driver_exc(self, exc):
        log.debug(traceback.format_exc())

        key = self.next_key()
        self.errors[key] = exc

        payload = {"id": key, "msg": ""}

        if isinstance(exc, MarkdAsDriverError):
            wrapped_exc = exc.wrapped_exc
            payload["errorType"] = str(type(wrapped_exc))
            if wrapped_exc.args:
                payload["msg"] = self._exc_msg(wrapped_exc.args[0])
            payload["retryable"] = False
        else:
            payload["errorType"] = str(type(exc))
            payload["msg"] = self._exc_msg(exc)
            if isinstance(exc, Neo4jError):
                payload["code"] = exc.code
            payload["retryable"] = getattr(exc, "is_retryable", bool)()

        self.send_response("DriverError", payload)

    def _process(self, request):
        # Process a received request.
        try:
            request = loads(request, object_pairs_hook=Request)
            if not isinstance(request, Request):
                raise TypeError("Request is not an object")
            name = request.get("name", "invalid")
            handler = self._requestHandlers.get(name)
            if not handler:
                raise ValueError("No request handler for " + name)
            data = request["data"]
            log.info("<<< " + name + dumps(data))
            handler(self, data)
            unused_keys = request.unseen_keys
            if unused_keys:
                raise NotImplementedError(
                    f"Backend does not support some properties of the {name} "
                    f"request: {', '.join(unused_keys)}"
                )
        except (
            Neo4jError,
            DriverError,
            UnsupportedServerProduct,
            BoltError,
            MarkdAsDriverError,
        ) as e:
            self.write_driver_exc(e)
        except requests.FrontendError as e:
            self.send_response("FrontendError", {"msg": str(e)})
        except Exception as e:
            if self._exc_stems_from_driver(e):
                self.write_driver_exc(e)
            else:
                tb = traceback.format_exc()
                log.error(tb)
                self.send_response("BackendError", {"msg": tb})

    def send_response(self, name, data):
        """Send a response to backend."""
        with buffer_handler.lock:
            log_output = buffer_handler.stream.getvalue()
            buffer_handler.stream.truncate(0)
            buffer_handler.stream.seek(0)
        if not log_output.endswith("\n"):
            log_output += "\n"
        self._wr.write(log_output.encode("utf-8"))
        response = {"name": name, "data": data}
        response = dumps(response)
        self._wr.write(b"#response begin\n")
        self._wr.write(bytes(response + "\n", "utf-8"))
        self._wr.write(b"#response end\n")
        if isinstance(self._wr, asyncio.StreamWriter):
            self._wr.drain()
        else:
            self._wr.flush()
        log.info(">>> " + name + dumps(data))
