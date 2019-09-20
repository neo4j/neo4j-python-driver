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
    "DEFAULT_PORT",
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
    "Auth",
    "AuthToken",
    "Security",
]

from logging import getLogger
from urllib.parse import urlparse, parse_qs

from neo4j.addressing import Address
from neo4j.api import *
from neo4j.exceptions import ConnectionExpired, ServiceUnavailable
from neo4j.meta import experimental, version as __version__


DEFAULT_PORT = 7687


# Auth
TRUST_ON_FIRST_USE = 0  # Deprecated
TRUST_SIGNED_CERTIFICATES = 1  # Deprecated
TRUST_ALL_CERTIFICATES = 2
TRUST_CUSTOM_CA_SIGNED_CERTIFICATES = 3
TRUST_SYSTEM_CA_SIGNED_CERTIFICATES = 4
TRUST_DEFAULT = TRUST_ALL_CERTIFICATES


# Connection Pool Management
INFINITE = -1
DEFAULT_MAX_CONNECTION_LIFETIME = 3600  # 1h
DEFAULT_MAX_CONNECTION_POOL_SIZE = 100
DEFAULT_CONNECTION_TIMEOUT = 5.0  # 5s


# Connection Settings
DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = 60  # 1m


# Routing settings
DEFAULT_MAX_RETRY_TIME = 30.0  # 30s


READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"


default_config = {
    "auth": None,
    "trust": TRUST_DEFAULT,
    "der_encoded_server_certificate": None,

    "user_agent": None,

    # Connection pool management
    "max_connection_lifetime": DEFAULT_MAX_CONNECTION_LIFETIME,
    "max_connection_pool_size": DEFAULT_MAX_CONNECTION_POOL_SIZE,
    "connection_acquisition_timeout": DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,

    # Connection settings:
    "connection_timeout": DEFAULT_CONNECTION_TIMEOUT,
    "keep_alive": True,

    # Routing settings:
    "max_retry_time": DEFAULT_MAX_RETRY_TIME,
}


log = getLogger("neo4j")


# For backwards compatibility
AuthToken = Auth


class Neo4j:
    """ Accessor for :class:`.Driver` construction.
    """

    @classmethod
    def driver(cls, uri, **config):
        """ Create a :class:`.Driver` object. Calling this method provides
        identical functionality to constructing a :class:`.Driver` or
        :class:`.Driver` subclass instance directly.
        """
        return Driver(uri, **config)


GraphDatabase = Neo4j


class Driver:
    """ Base class for all types of :class:`.Driver`, instances of which are
    used as the primary access point to Neo4j.

    :param uri: URI for a graph database service
    :param config: configuration and authentication details
    """

    #: Overridden by subclasses to specify the URI scheme owned by that
    #: class.
    uri_schemes = ()

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
        if parsed.scheme not in cls.uri_schemes:
            raise ValueError("%s objects require the one of the URI "
                             "schemes %r" % (cls.__name__, cls.uri_schemes))

    def __new__(cls, uri, **config):
        parsed = urlparse(uri)
        parsed_scheme = parsed.scheme
        for subclass in Driver.__subclasses__():
            assert issubclass(subclass, Driver)
            if parsed_scheme in subclass.uri_schemes:
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
        raise NotImplementedError("Blocking sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def async_session(self, **parameters):
        raise NotImplementedError("Asynchronous sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **parameters):
        """ Create a new :class:`.Pipeline` objects based on this
        :class:`.Driver`.
        """
        raise NotImplementedError("Pipelines are not implemented "
                                  "for the %s class" % type(self).__name__)

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

    uri_schemes = ("bolt",)

    def __new__(cls, uri, *, security=None, **config):
        from neo4j.io import Bolt, BoltPool
        cls._check_uri(uri)
        instance = object.__new__(cls)
        # We keep the address containing the host name or IP address exactly
        # as-is from the original URI. This means that every new connection
        # will carry out DNS resolution, leading to the possibility that
        # the connection pool may contain multiple IP address keys, one for
        # an old address and one for a new address.
        parsed = urlparse(uri)
        instance.address = Address.parse(parsed.netloc,
                                         default_port=DEFAULT_PORT)
        instance.encrypted = bool(security)

        def connector(address, **kwargs):
            return Bolt.open(address, **dict(config, **kwargs))

        pool = BoltPool(connector, instance.address, **config)
        pool.release(pool.acquire())
        instance._pool = pool
        instance._max_retry_time = config.get("max_retry_time",
                                              default_config["max_retry_time"])
        return instance

    def session(self, **parameters):
        self._assert_open()
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self._max_retry_time
        from neo4j.blocking import Session
        return Session(self._pool.acquire, **parameters)

    def async_session(self, **parameters):
        raise NotImplementedError("Asynchronous sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def pipeline(self, **parameters):
        from neo4j.pipelining import Pipeline
        return Pipeline(self._pool.acquire, **parameters)


class RoutingDriver(Driver):
    """ A :class:`.RoutingDriver` is created from a ``neo4j`` URI. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_ feature
    by directing read and write behaviour to appropriate cluster members.
    """

    uri_schemes = ("neo4j", "bolt+routing")

    @classmethod
    def parse_routing_context(cls, uri):
        query = urlparse(uri).query
        if not query:
            return {}

        context = {}
        parameters = parse_qs(query, True)
        for key in parameters:
            value_list = parameters[key]
            if len(value_list) != 1:
                raise ValueError("Duplicated query parameters with key '%s', "
                                 "value '%s' found in URL '%s'" % (key,
                                                                   value_list,
                                                                   uri))
            value = value_list[0]
            if not value:
                raise ValueError("Invalid parameters:'%s=%s' in URI "
                                 "'%s'." % (key, value, uri))
            context[key] = value
        return context

    def __new__(cls, uri, *, security=None, **config):
        from neo4j.io import Bolt, Neo4jPool
        cls._check_uri(uri)
        instance = object.__new__(cls)
        parsed = urlparse(uri)
        instance.initial_address = initial_address = \
            Address.parse(parsed.netloc, default_port=DEFAULT_PORT)
        instance.encrypted = bool(security)
        routing_context = cls.parse_routing_context(uri)

        def connector(address, **kwargs):
            return Bolt.open(address, **dict(config, **kwargs))

        pool = Neo4jPool(connector, initial_address,
                         routing_context, initial_address, **config)
        try:
            pool.update_routing_table()
        except Exception:
            pool.close()
            raise
        else:
            instance._pool = pool
            instance._max_retry_time = \
                config.get("max_retry_time", default_config["max_retry_time"])
            return instance

    def session(self, **parameters):
        self._assert_open()
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self._max_retry_time
        from neo4j.blocking import Session
        return Session(self._pool.acquire, **parameters)

    def async_session(self, **parameters):
        raise NotImplementedError("Asynchronous sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def pipeline(self, **parameters):
        raise NotImplementedError("Pipelines are not implemented "
                                  "for the %s class" % type(self).__name__)


class DriverError(Exception):
    """ Raised when an error occurs while using a driver.
    """

    def __init__(self, driver, *args):
        super(DriverError, self).__init__(*args)
        self.driver = driver


def basic_auth(user, password, realm=None):
    """ Generate a basic auth token for a given user and password.

    :param user: user name
    :param password: current password
    :param realm: specifies the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return Auth("basic", user, password, realm)


def kerberos_auth(base64_encoded_ticket):
    """ Generate a kerberos auth token with the base64 encoded ticket

    :param base64_encoded_ticket: a base64 encoded service ticket
    :return: an authentication token that can be used to connect to Neo4j
    """
    return Auth("kerberos", "", base64_encoded_ticket)


def custom_auth(principal, credentials, realm, scheme, **parameters):
    """ Generate a basic auth token for a given user and password.

    :param principal: specifies who is being authenticated
    :param credentials: authenticates the principal
    :param realm: specifies the authentication provider
    :param scheme: specifies the type of authentication
    :param parameters: parameters passed along to the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return Auth(scheme, principal, credentials, realm, **parameters)


class Workspace:

    def __init__(self, acquirer, **parameters):
        self._acquirer = acquirer
        self._parameters = parameters
        self._connection = None
        self._closed = False

    def __del__(self):
        try:
            self.close()
        except OSError:
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
        if self._connection:
            if sync:
                try:
                    self._connection.send_all()
                    self._connection.fetch_all()
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
