#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


class Structure(list):

    def __init__(self, capacity, signature):
        self.capacity = capacity
        self.signature = signature

    def __repr__(self):
        return repr(tuple(iter(self)))

    def __eq__(self, other):
        return list(self) == list(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        yield self.signature
        yield tuple(super(Structure, self).__iter__())
