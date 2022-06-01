# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


class TrustStore:
    # Base class for trust stores. For internal type-checking only.
    pass


class TrustSystemCAs(TrustStore):
    """Used to configure the driver to trust system CAs (default).

    Trust server certificates that can be verified against the system
    certificate authority. This option is primarily intended for use with
    full certificates.

    For example::

        import noe4j

        driver = neo4j.GraphDatabase.driver(
            url, auth=auth, trusted_certificates=neo4j.TrustSystemCAs()
        )
    """
    pass


class TrustAll(TrustStore):
    """Used to configure the driver to trust all certificates.

    Trust any server certificate. This ensures that communication
    is encrypted but does not verify the server certificate against a
    certificate authority. This option is primarily intended for use with
    the default auto-generated server certificate.


    For example::

        import noe4j

        driver = neo4j.GraphDatabase.driver(
            url, auth=auth, trusted_certificates=neo4j.TrustAll()
        )
    """
    pass


class TrustCustomCAs(TrustStore):
    """Used to configure the driver to trust custom CAs.

    Trust server certificates that can be verified against the certificate
    authority at the specified paths. This option is primarily intended for
    self-signed and custom certificates.

    :param certificates (str): paths to the certificates to trust.
        Those are not the certificates you expect to see from the server but
        the CA certificates you expect to be used to sign the server's
        certificate.

    For example::

        import noe4j

        driver = neo4j.GraphDatabase.driver(
            url, auth=auth,
            trusted_certificates=neo4j.TrustCustomCAs(
                "/path/to/ca1.crt", "/path/to/ca2.crt",
            )
        )
    """
    def __init__(self, *certificates):
        self.certs = certificates
