#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from asyncio import Event
from collections import deque


class WaitingList:

    def __init__(self, *, loop=None):
        self._loop = loop
        self._wait_list = deque()

    async def join(self):
        event = Event(loop=self._loop)
        self._wait_list.append(event)
        await event.wait()

    def notify(self):
        try:
            event = self._wait_list.popleft()
        except IndexError:
            pass
        else:
            event.set()
