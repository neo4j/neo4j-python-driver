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

from warnings import warn

from neo4j.bolt.connection import connect, ConnectionPool, DEFAULT_PORT, ProtocolError
from neo4j.compat import urlparse
from neo4j.compat.ssl import SSL_AVAILABLE, SSLContext, PROTOCOL_SSLv23, OP_NO_SSLv2, CERT_REQUIRED

from .routing import RoutingConnectionPool
from .session import Session


ENCRYPTION_OFF = 0
ENCRYPTION_ON = 1
ENCRYPTION_DEFAULT = ENCRYPTION_ON if SSL_AVAILABLE else ENCRYPTION_OFF

TRUST_ON_FIRST_USE = 0          # Deprecated
TRUST_SIGNED_CERTIFICATES = 1   # Deprecated
TRUST_ALL_CERTIFICATES = 2
TRUST_CUSTOM_CA_SIGNED_CERTIFICATES = 3
TRUST_SYSTEM_CA_SIGNED_CERTIFICATES = 4
TRUST_DEFAULT = TRUST_ALL_CERTIFICATES

READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"
DEFAULT_ACCESS = WRITE_ACCESS


class AuthToken(object):
    """ Container for auth information
    """

    #: By default we should not send any realm
    realm = None

    def __init__(self, scheme, principal, credentials, realm=None, **parameters):
        self.scheme = scheme
        self.principal = principal
        self.credentials = credentials
        if realm:
            self.realm = realm
        if parameters:
            self.parameters = parameters


class GraphDatabase(object):
    """ The :class:`.GraphDatabase` class provides access to all graph
    database functionality. This is primarily used to construct a driver
    instance, using the :meth:`.driver` method.
    """

    @staticmethod
    def driver(uri, **config):
        """ Acquire a :class:`.Driver` instance for the given URL and
        configuration:

            >>> from neo4j.v1 import GraphDatabase
            >>> driver = GraphDatabase.driver("bolt://localhost:7687")

        :param uri: URI for a graph database
        :param config: configuration and authentication details (valid keys are listed below)

            `auth`
              An authentication token for the server, for example
              ``basic_auth("neo4j", "password")``.

            `der_encoded_server_certificate`
              The server certificate in DER format, if required.

            `encrypted`
              Encryption level: one of :attr:`.ENCRYPTION_ON`, :attr:`.ENCRYPTION_OFF`
              or :attr:`.ENCRYPTION_NON_LOCAL`. The default setting varies
              depending on whether SSL is available or not. If it is,
              :attr:`.ENCRYPTION_NON_LOCAL` is the default.

            `trust`
              Trust level: one of :attr:`.TRUST_ON_FIRST_USE` (default) or
              :attr:`.TRUST_SIGNED_CERTIFICATES`.

            `user_agent`
              A custom user agent string, if required.

        """
        parsed = urlparse(uri)
        if parsed.scheme == "bolt":
            return DirectDriver((parsed.hostname, parsed.port or DEFAULT_PORT), **config)
        elif parsed.scheme == "bolt+routing":
            return RoutingDriver((parsed.hostname, parsed.port or DEFAULT_PORT), **config)
        else:
            raise ProtocolError("URI scheme %r not supported" % parsed.scheme)


class SecurityPlan(object):

    @classmethod
    def build(cls, **config):
        encrypted = config.get("encrypted", None)
        if encrypted is None:
            encrypted = _encryption_default()
        trust = config.get("trust", TRUST_DEFAULT)
        if encrypted:
            if not SSL_AVAILABLE:
                raise RuntimeError("Bolt over TLS is only available in Python 2.7.9+ and "
                                   "Python 3.3+")
            ssl_context = SSLContext(PROTOCOL_SSLv23)
            ssl_context.options |= OP_NO_SSLv2
            if trust == TRUST_ON_FIRST_USE:
                warn("TRUST_ON_FIRST_USE is deprecated, please use "
                     "TRUST_ALL_CERTIFICATES instead")
            elif trust == TRUST_SIGNED_CERTIFICATES:
                warn("TRUST_SIGNED_CERTIFICATES is deprecated, please use "
                     "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES instead")
                ssl_context.verify_mode = CERT_REQUIRED
            elif trust == TRUST_ALL_CERTIFICATES:
                pass
            elif trust == TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                raise NotImplementedError("Custom CA support is not implemented")
            elif trust == TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                ssl_context.verify_mode = CERT_REQUIRED
            else:
                raise ValueError("Unknown trust mode")
            ssl_context.set_default_verify_paths()
        else:
            ssl_context = None
        return cls(encrypted, ssl_context, trust != TRUST_ON_FIRST_USE)

    def __init__(self, requires_encryption, ssl_context, routing_compatible):
        self.encrypted = bool(requires_encryption)
        self.ssl_context = ssl_context
        self.routing_compatible = routing_compatible


class Driver(object):
    """ A :class:`.Driver` is an accessor for a specific graph database
    resource. It is thread-safe, acts as a template for sessions and hosts
    a connection pool.

    All configuration and authentication settings are held immutably by the
    `Driver`. Should different settings be required, a new `Driver` instance
    should be created via the :meth:`.GraphDatabase.driver` method.
    """

    pool = None

    def __init__(self, pool):
        self.pool = pool

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def session(self, access_mode=None):
        """ Create a new session using a connection from the driver connection
        pool. Session creation is a lightweight operation and sessions are
        not thread safe, therefore a session should generally be short-lived
        within a single thread.
        """
        pass

    def close(self):
        if self.pool:
            self.pool.close()
            self.pool = None


class DirectDriver(Driver):
    """ A :class:`.DirectDriver` is created from a `bolt` URI and addresses
    a single database instance.
    """

    def __init__(self, address, **config):
        self.address = address
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        pool = ConnectionPool(lambda a: connect(a, security_plan.ssl_context, **config))
        Driver.__init__(self, pool)

    def session(self, access_mode=None):
        return Session(self.pool.acquire(self.address))


class RoutingDriver(Driver):
    """ A :class:`.RoutingDriver` is created from a `bolt+routing` URI.
    """

    def __init__(self, address, **config):
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        if not security_plan.routing_compatible:
            # this error message is case-specific as there is only one incompatible
            # scenario right now
            raise ValueError("TRUST_ON_FIRST_USE is not compatible with routing")

        def connector(a):
            return connect(a, security_plan.ssl_context, **config)

        pool = RoutingConnectionPool(connector, address)
        try:
            pool.update_routing_table()
        except:
            pool.close()
            raise
        else:
            Driver.__init__(self, pool)

    def session(self, access_mode=None):
        if access_mode == READ_ACCESS:
            connection = self.pool.acquire_for_read()
        else:
            connection = self.pool.acquire_for_write()
        return Session(connection, access_mode)


def basic_auth(user, password, realm=None):
    """ Generate a basic auth token for a given user and password.

    :param user: user name
    :param password: current password
    :param realm: specifies the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return AuthToken("basic", user, password, realm)


def custom_auth(principal, credentials, realm, scheme, **parameters):
    """ Generate a basic auth token for a given user and password.

    :param principal: specifies who is being authenticated
    :param credentials: authenticates the principal
    :param realm: specifies the authentication provider
    :param scheme: specifies the type of authentication
    :param parameters: parameters passed along to the authenticatin provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return AuthToken(scheme, principal, credentials, realm, **parameters)


_warned_about_insecure_default = False


def _encryption_default():
    global _warned_about_insecure_default
    if not SSL_AVAILABLE and not _warned_about_insecure_default:
        warn("Bolt over TLS is only available in Python 2.7.9+ and Python 3.3+ "
             "so communications are not secure")
        _warned_about_insecure_default = True
    return ENCRYPTION_DEFAULT
