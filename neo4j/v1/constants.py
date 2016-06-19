#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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


from os.path import expanduser, join

from ..meta import version
from .ssl_compat import SSL_AVAILABLE


DEFAULT_PORT = 7687
DEFAULT_USER_AGENT = "neo4j-python/%s" % version

KNOWN_HOSTS = join(expanduser("~"), ".neo4j", "known_hosts")

MAGIC_PREAMBLE = 0x6060B017

ENCRYPTED_DEFAULT = SSL_AVAILABLE

TRUST_ON_FIRST_USE = 0
TRUST_SIGNED_CERTIFICATES = 1

TRUST_DEFAULT = TRUST_ON_FIRST_USE

DISCOVER_MEMBERS_CALL = "CALL dbms.cluster.discoverMembers"
ACQUIRE_ENDPOINTS_CALL = "CALL dbms.cluster.acquireEndpoints"

READ_ONLY_CONCURRENCY = -1
READ_WRITE_CONCURRENCY = 0
READ_YOUR_OWN_WRITES_CONCURRENCY = 1
DEFAULT_CONCURRENCY_LEVEL = READ_WRITE_CONCURRENCY
