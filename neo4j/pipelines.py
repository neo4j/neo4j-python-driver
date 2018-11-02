#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from collections import deque
from threading import Thread
from time import sleep


class WorkspaceError(Exception):

    pass


class Workspace(object):

    def __init__(self, acquirer, access_mode, **parameters):
        self._acquirer = acquirer
        self._default_access_mode = access_mode
        self._parameters = parameters
        self._connection = None
        self._closed = False

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode=None):
        if access_mode is None:
            access_mode = self._default_access_mode
        if self._connection:
            if access_mode == self._connection_access_mode:
                return
            self._disconnect(sync=True)
        self._connection = self._acquirer(access_mode)
        self._connection_access_mode = access_mode

    def _disconnect(self, sync):
        from neobolt.exceptions import ConnectionExpired, ServiceUnavailable
        if self._connection:
            if sync:
                try:
                    self._connection.sync()
                except (WorkspaceError, ConnectionExpired, ServiceUnavailable):
                    pass
            if self._connection:
                self._connection.in_use = False
                self._connection = None
            self._connection_access_mode = None

    def close(self):
        try:
            self._disconnect(sync=True)
        finally:
            self._closed = True

    def closed(self):
        """ Indicator for whether or not this session has been closed.

        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed


class Pipeline(Workspace):

    def __init__(self, acquirer, access_mode, **parameters):
        super(Pipeline, self).__init__(acquirer, access_mode, **parameters)
        self._connect(access_mode)
        self._flush_every = parameters.get("flush_every", 8192)
        self._data = deque()

    def push(self, statement, parameters=None):
        self._connection.run(statement, parameters)
        self._connection.pull_all(on_records=self._data.extend)
        output_buffer_size = len(self._connection.output_buffer.view())
        if output_buffer_size >= self._flush_every:
            self._connection.send()

    def pull(self):
        summary = 0
        while summary == 0:
            detail, summary = self._connection.fetch()
        summary = 0
        while summary == 0:
            detail, summary = self._connection.fetch()
            if detail:
                yield self._data.popleft()


class Pusher(Thread):

    def __init__(self, pipeline):
        super(Pusher, self).__init__()
        self.pipeline = pipeline
        self.running = True
        self.count = 0

    def run(self):
        while self.running:
            self.pipeline.push("RETURN $x", {"x": self.count})
            self.count += 1


class Puller(Thread):

    def __init__(self, pipeline):
        super(Puller, self).__init__()
        self.pipeline = pipeline
        self.running = True
        self.count = 0

    def run(self):
        while self.running:
            for _ in self.pipeline.pull():
                pass    # consume and discard records
            self.count += 1


def main():
    from neo4j import Driver
    # from neobolt.diagnostics import watch
    # watch("neobolt")
    with Driver("bolt://", auth=("neo4j", "password")) as dx:
        p = dx.pipeline(flush_every=1024)
        pusher = Pusher(p)
        puller = Puller(p)
        try:
            pusher.start()
            puller.start()
            while True:
                print("sent %d, received %d, backlog %d" % (pusher.count, puller.count, pusher.count - puller.count))
                sleep(1)
        except KeyboardInterrupt:
            pusher.running = False
            pusher.join()
            puller.running = False
            puller.join()


if __name__ == "__main__":
    main()
