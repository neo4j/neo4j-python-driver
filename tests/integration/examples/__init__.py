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


# List of example tags used in the drivers manual version 4.0


# hello-world               - Example 1.6. Hello World
# driver-lifecycle          - Example 2.1. The driver lifecycle
# custom-resolver           - Example 2.2. Custom Address Resolver
# basic-auth                - Example 2.5. Basic authentication
# kerberos-auth             - Example 2.6. Kerberos authentication
# custom-auth               - Example 2.7. Custom authentication
# config-connection-pool    - Example 2.8. Configure connection pool
# config-connection-timeout - Example 2.9. Configure connection timeout
# config-unencrypted        - Example 2.10. Unencrypted configuration
# config-max-retry-time     - Example 2.11. Configure maximum transaction retry time
# config-trust              - Example 2.12. Configure trusted certificates
# pass-bookmarks            - Example 3.1. Pass bookmarks
# read-write-transaction    - Example 3.2. Read-write transaction
# database-selection        - Example 3.3. Database selection on session creation
# transaction-function      - Example 4.2. Transaction function
# session                   - Example 4.3. Simple auto-commit transactions
# result-consume            - Example 4.4. Consuming results
# result-retain             - Example 4.5. Retain results for further processing
#                           - Example 4.6. Asynchronous transaction functions
#                           - Example 4.7. Asynchronous auto-commit transactions
#                           - Example 4.8. Consuming results
#                           - Example 4.9. Reactive transaction functions
#                           - Example 4.10. Auto-commit transactions
#                           - Example 4.11. Consuming results


class DriverSetupExample:

    driver = None

    def close(self):
        if self.driver:
            self.driver.close()

    @classmethod
    def test(cls, *args, **kwargs):
        example = cls(*args, **kwargs)
        try:
            with example.driver.session() as session:
                assert session.run("RETURN 1").single().value() == 1
        finally:
            example.close()
