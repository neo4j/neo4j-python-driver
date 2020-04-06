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
from threading import Thread, Lock
from time import sleep

from neo4j.work import Workspace
from neo4j.conf import WorkspaceConfig
from neo4j.api import (
    WRITE_ACCESS,
)

class PipelineConfig(WorkspaceConfig):

    #:
    flush_every = 8192  # bytes


class Pipeline(Workspace):

    def __init__(self, pool, config):
        assert isinstance(config, PipelineConfig)
        super(Pipeline, self).__init__(pool, config)
        self._connect(WRITE_ACCESS)
        self._flush_every = config.flush_every
        self._data = deque()
        self._pull_lock = Lock()

    def push(self, statement, parameters=None):
        self._connection.run(statement, parameters)
        self._connection.pull(on_records=self._data.extend)
        output_buffer_size = len(self._connection.outbox.view())
        if output_buffer_size >= self._flush_every:
            self._connection.send_all()

    def _results_generator(self):
        results_returned_count = 0
        try:
            summary = 0
            while summary == 0:
                _, summary = self._connection.fetch_message()
            summary = 0
            while summary == 0:
                detail, summary = self._connection.fetch_message()
                for n in range(detail):
                    response = self._data.popleft()
                    results_returned_count += 1
                    yield response
        finally:
            self._pull_lock.release()

    def pull(self):
        """Returns a generator containing the results of the next query in the pipeline"""
        # n.b. pull is now somewhat misleadingly named because it doesn't do anything
        # the connection isn't touched until you try and iterate the generator we return
        lock_acquired = self._pull_lock.acquire(blocking=False)
        if not lock_acquired:
            raise PullOrderException()
        return self._results_generator()


class PullOrderException(Exception):
    """Raise when calling pull if a previous pull result has not been fully consumed"""


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
    # from neo4j.bolt.diagnostics import watch
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
