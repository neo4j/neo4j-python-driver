# Copyright (c) 2002-2020 "Neo4j,"
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
from json import loads, dumps
from inspect import getmembers, isfunction
import testkitbackend.requests as requests


class Backend:
    def __init__(self, rd, wr):
        self._rd = rd
        self._wr = wr
        self.drivers = {}
        self.sessions = {}
        self.results = {}
        self.transactions = {}
        self.key = 0
        # Collect all request handlers
        self._requestHandlers = dict(
            [m for m in getmembers(requests, isfunction)])

    def next_key(self):
        self.key = self.key + 1
        return self.key

    def process_request(self):
        """ Reads next request from the stream and processes it.
        """
        in_request = False
        request = ""
        for line in self._rd:
            # Remove trailing newline
            line = line.decode('UTF-8').rstrip()
            if line == "#request begin":
                in_request = True
            elif line == "#request end":
                self._process(request)
                return True
            else:
                if in_request:
                    request = request + line
        return False

    def _process(self, request):
        """ Process a received request by retrieving handler that
        corresponds to the request name.
        """
        request = loads(request)
        if not isinstance(request, dict):
            raise Exception("Request is not an object")
        name = request.get('name', 'invalid')
        handler = self._requestHandlers.get(name)
        if not handler:
            raise Exception("No request handler for " + name)
        data = request["data"]
        handler(self, data)

    def send_response(self, name, data):
        """ Sends a response to backend.
        """
        response = {"name": name, "data": data}
        response = dumps(response)
        self._wr.write(b"#response begin\n")
        self._wr.write(bytes(response+"\n", "utf-8"))
        self._wr.write(b"#response end\n")
        self._wr.flush()
