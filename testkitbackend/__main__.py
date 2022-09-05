#!/usr/bin/env python

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
import sys
import warnings

from .server import (
    AsyncServer,
    Server,
)


def sync_main():
    server = Server(("0.0.0.0", 9876))
    while True:
        server.handle_request()


def async_main():
    async def main():
        server = AsyncServer(("0.0.0.0", 9876))
        await server.start()
        try:
            await server.serve_forever()
        finally:
            server.stop()

    asyncio.run(main())


if __name__ == "__main__":
    warnings.simplefilter("error")
    if len(sys.argv) == 2 and sys.argv[1].lower().strip() == "async":
        async_main()
    else:
        sync_main()
