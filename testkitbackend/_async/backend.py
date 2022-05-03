# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


import asyncio
from inspect import (
    getmembers,
    isfunction,
)
from json import (
    dumps,
    loads,
)
from pathlib import Path
import traceback

from neo4j._exceptions import BoltError
from neo4j.exceptions import (
    DriverError,
    Neo4jError,
    UnsupportedServerProduct,
)

from . import requests
from .._driver_logger import (
    buffer_handler,
    log,
)
from ..backend import Request
from ..exceptions import MarkdAsDriverException


TESTKIT_BACKEND_PATH = Path(__file__).absolute().resolve().parents[1]
DRIVER_PATH = TESTKIT_BACKEND_PATH.parent / "neo4j"


class AsyncBackend:
    def __init__(self, rd, wr):
        self._rd = rd
        self._wr = wr
        self.drivers = {}
        self.custom_resolutions = {}
        self.dns_resolutions = {}
        self.sessions = {}
        self.results = {}
        self.errors = {}
        self.transactions = {}
        self.errors = {}
        self.key = 0
        # Collect all request handlers
        self._requestHandlers = dict(
            [m for m in getmembers(requests, isfunction)])

    def next_key(self):
        self.key = self.key + 1
        return self.key

    async def process_request(self):
        """ Reads next request from the stream and processes it.
        """
        in_request = False
        request = ""
        async for line in self._rd:
            # Remove trailing newline
            line = line.decode('UTF-8').rstrip()
            if line == "#request begin":
                in_request = True
            elif line == "#request end":
                await self._process(request)
                return True
            else:
                if in_request:
                    request = request + line
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

    async def write_driver_exc(self, exc):
        log.debug(traceback.format_exc())

        key = self.next_key()
        self.errors[key] = exc

        payload = {"id": key, "msg": ""}

        if isinstance(exc, MarkdAsDriverException):
            wrapped_exc = exc.wrapped_exc
            payload["errorType"] = str(type(wrapped_exc))
            if wrapped_exc.args:
                payload["msg"] = str(wrapped_exc.args[0])
        else:
            payload["errorType"] = str(type(exc))
            if isinstance(exc, Neo4jError) and exc.message is not None:
                payload["msg"] = str(exc.message)
            elif exc.args:
                payload["msg"] = str(exc.args[0])

            if isinstance(exc, Neo4jError):
                payload["code"] = exc.code

        await self.send_response("DriverError", payload)

    async def _process(self, request):
        """ Process a received request by retrieving handler that
        corresponds to the request name.
        """
        try:
            request = loads(request, object_pairs_hook=Request)
            if not isinstance(request, Request):
                raise Exception("Request is not an object")
            name = request.get('name', 'invalid')
            handler = self._requestHandlers.get(name)
            if not handler:
                raise Exception("No request handler for " + name)
            data = request["data"]
            log.info("<<< " + name + dumps(data))
            await handler(self, data)
            unsused_keys = request.unseen_keys
            if unsused_keys:
                raise NotImplementedError(
                    "Backend does not support some properties of the " + name +
                    " request: " + ", ".join(unsused_keys)
                )
        except (Neo4jError, DriverError, UnsupportedServerProduct,
                BoltError, MarkdAsDriverException) as e:
            await self.write_driver_exc(e)
        except requests.FrontendError as e:
            await self.send_response("FrontendError", {"msg": str(e)})
        except Exception as e:
            if self._exc_stems_from_driver(e):
                await self.write_driver_exc(e)
            else:
                tb = traceback.format_exc()
                log.error(tb)
                await self.send_response("BackendError", {"msg": tb})

    async def send_response(self, name, data):
        """ Sends a response to backend.
        """
        with buffer_handler.lock:
            log_output = buffer_handler.stream.getvalue()
            buffer_handler.stream.truncate(0)
            buffer_handler.stream.seek(0)
        if not log_output.endswith("\n"):
            log_output += "\n"
        self._wr.write(log_output.encode("utf-8"))
        response = {"name": name, "data": data}
        response = dumps(response)
        log.info(">>> " + name + dumps(data))
        self._wr.write(b"#response begin\n")
        self._wr.write(bytes(response+"\n", "utf-8"))
        self._wr.write(b"#response end\n")
        if isinstance(self._wr, asyncio.StreamWriter):
            await self._wr.drain()
        else:
            self._wr.flush()
