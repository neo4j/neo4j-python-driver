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


__all__ = [
    "__version__",
    "READ_ACCESS",
    "WRITE_ACCESS",
    "TRUST_ALL_CERTIFICATES",
    "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
    "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
    "GraphDatabase",
    "Driver",
    "DirectDriver",
    "RoutingDriver",
    "Workspace",
    "WorkspaceError",
    "DriverError",
    "basic_auth",
    "custom_auth",
    "kerberos_auth",
]

try:
    from neobolt.exceptions import (
        CypherError,
        TransientError,
        ServiceUnavailable,
    )
except ImportError:
    # We allow this to fail because this module can be imported implicitly
    # during setup. At that point, dependencies aren't available.
    pass
else:
    __all__.extend([
        "CypherError",
        "TransientError",
        "ServiceUnavailable",
    ])

from urllib.parse import urlparse
from warnings import warn


from .config import *
from .meta import experimental, version as __version__


READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"


class GraphDatabase(object):
    """ Accessor for :class:`.Driver` construction.
    """

    @classmethod
    def driver(cls, uri, **config):
        """ Create a :class:`.Driver` object. Calling this method provides
        identical functionality to constructing a :class:`.Driver` or
        :class:`.Driver` subclass instance directly.
        :param uri: the URL to a Neo4j instance
        :param config: user defined configuration
        :return: a new driver to the database instance specified by the URL
        """
        return Driver(uri, **config)

    @classmethod
    def routing_driver(cls, routing_uris, **config):
        """ Create a :class`.RoutingDriver` object from the first available address.
        :param routing_uris: List or comma separated list of URIs for Neo4j instances. All given URIs should
        have 'neo4j' scheme
        :param config: user defined configuration
        :return: a new driver instance
        """
        if isinstance(routing_uris, list):
            uris = routing_uris
        else:
            uris = routing_uris.split(",")
        for uri in uris:
            try:
                return RoutingDriver(uri, **config)
            except ServiceUnavailable:
                warn(f"Unable to create routing driver for URI: {uri}")


class Driver(object):
    """ Base class for all types of :class:`.Driver`, instances of which are
    used as the primary access point to Neo4j.

    :param uri: URI for a graph database service
    :param config: configuration and authentication details (valid keys are listed below)
    """

    #: Overridden by subclasses to specify the URI scheme owned by that
    #: class.
    uri_scheme = None

    #: Connection pool
    _pool = None

    #: Indicator of driver closure.
    _closed = False

    @classmethod
    def _check_uri(cls, uri):
        """ Check whether a URI is compatible with a :class:`.Driver`
        subclass. When called from a subclass, execution simply passes
        through if the URI scheme is valid for that class. If invalid,
        a `ValueError` is raised.

        :param uri: URI to check for compatibility
        :raise: `ValueError` if URI scheme is incompatible
        """
        parsed = urlparse(uri)
        if parsed.scheme != cls.uri_scheme:
            raise ValueError("%s objects require the %r URI scheme" % (cls.__name__, cls.uri_scheme))

    def __new__(cls, uri, **config):
        parsed = urlparse(uri)
        parsed_scheme = parsed.scheme
        if parsed_scheme == "bolt+routing":
            warn("The 'bolt+routing' URI scheme is deprecated, please use the 'neo4j' scheme instead")
            parsed_scheme = "neo4j"
        for subclass in Driver.__subclasses__():
            if parsed_scheme == subclass.uri_scheme:
                return subclass(uri, **config)
        raise ValueError("URI scheme %r not supported" % parsed.scheme)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _assert_open(self):
        if self.closed():
            raise DriverError("Driver closed")

    def session(self, **parameters):
        """ Create a new :class:`.Session` object based on this
        :class:`.Driver`.

        :param parameters: custom session parameters (see
                           :class:`.Session` for details)
        :returns: new :class:`.Session` object
        """
        raise NotImplementedError("Blocking sessions are not implemented for the %s class" % type(self).__name__)

    def async_session(self, **parameters):
        raise NotImplementedError("Asynchronous sessions are not implemented for the %s class" % type(self).__name__)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented for the %s class" % type(self).__name__)

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **parameters):
        """ Create a new :class:`.Pipeline` objects based on this
        :class:`.Driver`.
        """
        raise NotImplementedError("Pipelines are not implemented for the %s class" % type(self).__name__)

    def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        if not self._closed:
            self._closed = True
            if self._pool is not None:
                self._pool.close()
                self._pool = None

    def closed(self):
        """ Return :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed


class DirectDriver(Driver):
    """ A :class:`.DirectDriver` is created from a ``bolt`` URI and addresses
    a single database machine. This may be a standalone server or could be a
    specific member of a cluster.

    Connections established by a :class:`.DirectDriver` are always made to the
    exact host and port detailed in the URI.
    """

    uri_scheme = "bolt"

    def __new__(cls, uri, **config):
        from neobolt.addressing import SocketAddress
        from neobolt.direct import ConnectionPool, DEFAULT_PORT, connect
        from neobolt.security import ENCRYPTION_OFF, ENCRYPTION_ON, SSL_AVAILABLE, SecurityPlan
        cls._check_uri(uri)
        if SocketAddress.parse_routing_context(uri):
            raise ValueError("Parameters are not supported with scheme 'bolt'. Given URI: '%s'." % uri)
        instance = object.__new__(cls)
        # We keep the address containing the host name or IP address exactly
        # as-is from the original URI. This means that every new connection
        # will carry out DNS resolution, leading to the possibility that
        # the connection pool may contain multiple IP address keys, one for
        # an old address and one for a new address.
        instance.address = SocketAddress.from_uri(uri, DEFAULT_PORT)
        if config.get("encrypted") is None:
            config["encrypted"] = ENCRYPTION_ON if SSL_AVAILABLE else ENCRYPTION_OFF
        instance.security_plan = security_plan = SecurityPlan.build(**config)
        instance.encrypted = security_plan.encrypted

        def connector(address, **kwargs):
            return connect(address, **dict(config, **kwargs))

        pool = ConnectionPool(connector, instance.address, **config)
        pool.release(pool.acquire())
        instance._pool = pool
        instance._max_retry_time = config.get("max_retry_time", default_config["max_retry_time"])
        return instance

    def session(self, **parameters):
        self._assert_open()
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self._max_retry_time
        from neo4j.blocking import Session
        return Session(self._pool.acquire, **parameters)

    def pipeline(self, **parameters):
        from .pipelining import Pipeline
        return Pipeline(self._pool.acquire, **parameters)


class RoutingDriver(Driver):
    """ A :class:`.RoutingDriver` is created from a ``neo4j`` URI. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_ feature
    by directing read and write behaviour to appropriate cluster members.
    """

    uri_scheme = "neo4j"

    def __new__(cls, uri, **config):
        from neobolt.addressing import SocketAddress
        from neobolt.direct import DEFAULT_PORT, connect
        from neobolt.routing import RoutingConnectionPool
        from neobolt.security import ENCRYPTION_OFF, ENCRYPTION_ON, SSL_AVAILABLE, SecurityPlan
        cls._check_uri(uri)
        instance = object.__new__(cls)
        instance.initial_address = initial_address = SocketAddress.from_uri(uri, DEFAULT_PORT)
        if config.get("encrypted") is None:
            config["encrypted"] = ENCRYPTION_ON if SSL_AVAILABLE else ENCRYPTION_OFF
        instance.security_plan = security_plan = SecurityPlan.build(**config)
        instance.encrypted = security_plan.encrypted
        routing_context = SocketAddress.parse_routing_context(uri)
        if not security_plan.routing_compatible:
            # this error message is case-specific as there is only one incompatible
            # scenario right now
            raise ValueError("TRUST_ON_FIRST_USE is not compatible with routing")

        def connector(address, **kwargs):
            return connect(address, **dict(config, **kwargs))

        pool = RoutingConnectionPool(connector, initial_address, routing_context, initial_address, **config)
        try:
            pool.update_routing_table()
        except:
            pool.close()
            raise
        else:
            instance._pool = pool
            instance._max_retry_time = config.get("max_retry_time", default_config["max_retry_time"])
            return instance

    def session(self, **parameters):
        self._assert_open()
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self._max_retry_time
        from neo4j.blocking import Session
        return Session(self._pool.acquire, **parameters)


class DriverError(Exception):
    """ Raised when an error occurs while using a driver.
    """

    def __init__(self, driver, *args, **kwargs):
        super(DriverError, self).__init__(*args, **kwargs)
        self.driver = driver


def basic_auth(user, password, realm=None):
    """ Generate a basic auth token for a given user and password.

    :param user: user name
    :param password: current password
    :param realm: specifies the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    from neobolt.security import AuthToken
    return AuthToken("basic", user, password, realm)


def kerberos_auth(base64_encoded_ticket):
    """ Generate a kerberos auth token with the base64 encoded ticket

    :param base64_encoded_ticket: a base64 encoded service ticket
    :return: an authentication token that can be used to connect to Neo4j
    """
    from neobolt.security import AuthToken
    return AuthToken("kerberos", "", base64_encoded_ticket)


def custom_auth(principal, credentials, realm, scheme, **parameters):
    """ Generate a basic auth token for a given user and password.

    :param principal: specifies who is being authenticated
    :param credentials: authenticates the principal
    :param realm: specifies the authentication provider
    :param scheme: specifies the type of authentication
    :param parameters: parameters passed along to the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    from neobolt.security import AuthToken
    return AuthToken(scheme, principal, credentials, realm, **parameters)


class Workspace(object):

    def __init__(self, acquirer, **parameters):
        self._acquirer = acquirer
        self._parameters = parameters
        self._connection = None
        self._closed = False

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode=None):
        if access_mode is None:
            access_mode = self._parameters.get("access_mode", "WRITE")
        if self._connection:
            if access_mode == self._connection_access_mode:
                return
            self._disconnect(sync=True)
        self._connection = self._acquirer(access_mode)
        self._connection_access_mode = access_mode

    def _disconnect(self, sync):
        from neobolt.exceptions import ConnectionExpired, ServiceUnavailable
        if self._connection:
            if sync:
                try:
                    self._connection.sync()
                except (WorkspaceError, ConnectionExpired, ServiceUnavailable):
                    pass
            if self._connection:
                self._connection.in_use = False
                self._connection = None
            self._connection_access_mode = None

    def close(self):
        try:
            self._disconnect(sync=True)
        finally:
            self._closed = True

    def closed(self):
        """ Indicator for whether or not this session has been closed.

        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed


class WorkspaceError(Exception):

    pass


from neo4j.types import *
