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


import asyncio
from socketserver import (
    StreamRequestHandler,
    TCPServer,
)

from ._async.backend import AsyncBackend
from ._sync.backend import Backend


class Server(TCPServer):
    allow_reuse_address = True

    def __init__(self, address):
        class Handler(StreamRequestHandler):
            def handle(self):
                backend = Backend(self.rfile, self.wfile)
                try:
                    while backend.process_request():
                        pass
                finally:
                    backend.close()
                print("Disconnected")
        super(Server, self).__init__(address, Handler)


class AsyncServer:
    def __init__(self, address):
        self._address = address
        self._server = None

    @staticmethod
    async def _handler(reader, writer):
        backend = AsyncBackend(reader, writer)
        try:
            while await backend.process_request():
                pass
        finally:
            writer.close()
            await backend.close()
        print("Disconnected")

    async def start(self):
        self._server = await asyncio.start_server(
            self._handler, host=self._address[0], port=self._address[1],
            limit=float("inf")  # this is dirty but works (for now)
        )

    async def serve_forever(self):
        if not self._server:
            raise RuntimeError("Server not started")
        await self._server.serve_forever()

    def stop(self):
        if not self._server:
            raise RuntimeError("Try starting the server before stopping it ;)")
        self._server.close()
