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


from ssl import SSLContext, PROTOCOL_SSLv23, OP_NO_SSLv2, CERT_REQUIRED


# TODO 2.0: tidy these up
TRUST_ALL_CERTIFICATES = 2
TRUST_CUSTOM_CA_SIGNED_CERTIFICATES = 3
TRUST_SYSTEM_CA_SIGNED_CERTIFICATES = 4
TRUST_DEFAULT = TRUST_ALL_CERTIFICATES


def make_ssl_context(**config):
    if config.get("encrypted") or config.get("secure"):
        ssl_context = SSLContext(PROTOCOL_SSLv23)
        ssl_context.options |= OP_NO_SSLv2
        trust = config.get("trust", TRUST_DEFAULT)
        if trust == TRUST_ALL_CERTIFICATES:
            pass
        elif trust == TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
            raise NotImplementedError("Custom CA support is not implemented")
        elif trust == TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            raise ValueError("Unknown trust mode")
        ssl_context.set_default_verify_paths()
        return ssl_context
    else:
        return None
