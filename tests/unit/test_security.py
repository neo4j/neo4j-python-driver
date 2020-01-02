#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from unittest import TestCase
from neo4j import kerberos_auth, basic_auth, custom_auth


class AuthTokenTestCase(TestCase):

    def test_should_generate_kerberos_auth_token_correctly(self):
        auth = kerberos_auth("I am a base64 service ticket")
        assert auth.scheme == "kerberos"
        assert auth.principal == ""
        assert auth.credentials == "I am a base64 service ticket"
        assert not auth.realm
        assert not hasattr(auth, "parameters")

    def test_should_generate_basic_auth_without_realm_correctly(self):
        auth = basic_auth("molly", "meoooow")
        assert auth.scheme == "basic"
        assert auth.principal == "molly"
        assert auth.credentials == "meoooow"
        assert not auth.realm
        assert not hasattr(auth, "parameters")

    def test_should_generate_base_auth_with_realm_correctly(self):
        auth = basic_auth("molly", "meoooow", "cat_cafe")
        assert auth.scheme == "basic"
        assert auth.principal == "molly"
        assert auth.credentials == "meoooow"
        assert auth.realm == "cat_cafe"
        assert not hasattr(auth, "parameters")

    def test_should_generate_custom_auth_correctly(self):
        auth = custom_auth("molly", "meoooow", "cat_cafe", "cat", age="1", color="white")
        assert auth.scheme == "cat"
        assert auth.principal == "molly"
        assert auth.credentials == "meoooow"
        assert auth.realm == "cat_cafe"
        assert auth.parameters == {"age": "1", "color": "white"}
