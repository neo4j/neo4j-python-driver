# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


# List of example tags used in the drivers manual version 4.0
#
# Example 1.6. Hello World
#     hello-world, hello-world-import
#
# Example 2.1. The driver lifecycle
#     driver-lifecycle, driver-lifecycle-import
#
# Example 2.2. Custom Address Resolver
#     custom-resolver, custom-resolver-import
#
# Example 2.5. Basic authentication
#     basic-auth, basic-auth-import
#
# Example 2.6. Kerberos authentication
#     kerberos-auth, kerberos-auth-import
#
# Example 2.7. Custom authentication
#     custom-auth, custom-auth-import
#
# Example 2.8. Configure connection pool
#     config-connection-pool, config-connection-pool-import
#
# Example 2.9. Configure connection timeout
#     config-connection-timeout, config-connection-timeout-import
#
# Example 2.10. Unencrypted configuration
#     config-unencrypted, config-unencrypted-import
#
# Example 2.11. Configure maximum transaction retry time
#     config-max-retry-time, config-max-retry-time-import
#
# Example 2.12. Configure trusted certificates
#     config-trust, config-trust-import
#
# Example 3.1. Pass bookmarks
#     pass-bookmarks, pass-bookmarks-import
#
# Example 3.2. Read-write transaction
#     read-write-transaction
#
# Example 3.3. Database selection on session creation
#     database-selection, database-selection-import
#
# Example 4.1. Session construction and closure
#     Hard coded (session)
#
# Example 4.2. Transaction function
#     transaction-function, transaction-function-import
#
# Example 4.3. Simple auto-commit transactions
#     autocommit-transaction, autocommit-transaction-import
#
# Example 4.4. Consuming results
#     result-consume
#
# Example 4.5. Retain results for further processing
#     result-retain
#
# Example 4.6. Asynchronous transaction functions
#     ---
#
# Example 4.7. Asynchronous auto-commit transactions
#     ---
#
# Example 4.8. Asynchronous consuming results
#     ---
#
# Example 4.9. Reactive transaction functions
#     ---
#
# Example 4.10.Reactive auto-commit transactions
#     ---
#
# Example 4.11.Reactive consuming results
#     ---


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
