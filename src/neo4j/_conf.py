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


from __future__ import annotations

from abc import ABCMeta
from collections.abc import Mapping

from ._meta import (
    deprecation_warn,
    experimental_warn,
)
from .api import (
    DEFAULT_DATABASE,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    WRITE_ACCESS,
)
from .exceptions import ConfigurationError


def iter_items(iterable):
    """ Iterate through all items (key-value pairs) within an iterable
    dictionary-like object. If the object has a `keys` method, this is
    used along with `__getitem__` to yield each pair in turn. If no
    `keys` method exists, each iterable element is assumed to be a
    2-tuple of key and value.
    """
    if hasattr(iterable, "keys"):
        for key in iterable.keys():
            yield key, iterable[key]
    else:
        for key, value in iterable:
            yield key, value


class TrustStore:
    # Base class for trust stores. For internal type-checking only.
    pass


class TrustSystemCAs(TrustStore):
    """Used to configure the driver to trust system CAs (default).

    Trust server certificates that can be verified against the system
    certificate authority. This option is primarily intended for use with
    full certificates.

    For example::

        import neo4j

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

    .. warning::
        This still leaves you vulnerable to man-in-the-middle attacks. It will
        just prevent eavesdropping "from the side-line" (i.e., without
        intercepting the connection).

    For example::

        import neo4j

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

        import neo4j

        driver = neo4j.GraphDatabase.driver(
            url, auth=auth,
            trusted_certificates=neo4j.TrustCustomCAs(
                "/path/to/ca1.crt", "/path/to/ca2.crt",
            )
        )
    """
    def __init__(self, *certificates):
        self.certs = certificates


class DeprecatedAlias:
    """Used when a config option has been renamed."""

    def __init__(self, new):
        self.new = new


class DeprecatedAlternative:
    """Used for deprecated config options that have a similar alternative."""

    def __init__(self, new, converter=None):
        self.new = new
        self.converter = converter


class DeprecatedOption:
    """Used for deprecated config options without alternative."""

    def __init__(self, value):
        self.value = value


class ExperimentalOption:
    """Used for experimental config options."""

    def __init__(self, value):
        self.value = value


class ConfigType(ABCMeta):

    def __new__(mcs, name, bases, attributes):
        fields = []
        deprecated_aliases = {}
        deprecated_alternatives = {}
        deprecated_options = {}
        experimental_options = {}

        for base in bases:
            if type(base) is mcs:
                fields += base.keys()
                deprecated_aliases.update(base._deprecated_aliases())
                deprecated_alternatives.update(base._deprecated_alternatives())
                deprecated_options.update(base._deprecated_options())
                experimental_options.update(base._experimental_options())

        for k, v in attributes.items():
            if (
                k.startswith("_")
                or callable(v)
                or isinstance(v, (staticmethod, classmethod))
            ):
                continue
            if isinstance(v, DeprecatedAlias):
                deprecated_aliases[k] = v.new
                continue
            if isinstance(v, DeprecatedAlternative):
                deprecated_alternatives[k] = v.new, v.converter
                continue
            fields.append(k)
            if isinstance(v, DeprecatedOption):
                deprecated_options[k] = v.value
                attributes[k] = v.value
                continue
            if isinstance(v, ExperimentalOption):
                experimental_options[k] = v.value
                attributes[k] = v.value
                continue

        def keys(_):
            return set(fields)

        def _deprecated_keys(_):
            return (set(deprecated_aliases.keys())
                    | set(deprecated_alternatives.keys()))

        def _get_new(_, key):
            return deprecated_aliases.get(
                key, deprecated_alternatives.get(key, (None,))[0]
            )

        def _deprecated_aliases(_):
            return deprecated_aliases

        def _deprecated_alternatives(_):
            return deprecated_alternatives

        def _deprecated_options(_):
            return deprecated_options

        def _experimental_options(_):
            return experimental_options

        attributes.setdefault("keys", classmethod(keys))
        attributes.setdefault("_get_new",
                              classmethod(_get_new))
        attributes.setdefault("_deprecated_keys",
                              classmethod(_deprecated_keys))
        attributes.setdefault("_deprecated_aliases",
                              classmethod(_deprecated_aliases))
        attributes.setdefault("_deprecated_alternatives",
                              classmethod(_deprecated_alternatives))
        attributes.setdefault("_deprecated_options",
                              classmethod(_deprecated_options))
        attributes.setdefault("_experimental_options",
                              classmethod(_experimental_options))

        return super(ConfigType, mcs).__new__(
            mcs, name, bases, {k: v for k, v in attributes.items()
                               if k not in _deprecated_keys(None)}
        )


class Config(Mapping, metaclass=ConfigType):
    """ Base class for all configuration containers.
    """

    @staticmethod
    def consume_chain(data, *config_classes):
        values = []
        for config_class in config_classes:
            if not issubclass(config_class, Config):
                raise TypeError("%r is not a Config subclass" % config_class)
            values.append(config_class._consume(data))
        if data:
            raise ConfigurationError("Unexpected config keys: %s" % ", ".join(data.keys()))
        return values

    @classmethod
    def consume(cls, data):
        config, = cls.consume_chain(data, cls)
        return config

    @classmethod
    def _consume(cls, data):
        config = {}
        if data:
            for key in cls.keys() | cls._deprecated_keys():
                try:
                    value = data.pop(key)
                except KeyError:
                    pass
                else:
                    config[key] = value
        return cls(config)

    def __update(self, data, warn=True):
        data_dict = dict(iter_items(data))

        def set_attr(k, v):
            if k in self.keys():
                if warn and k in self._deprecated_options():
                    deprecation_warn("The '{}' config key is "
                                     "deprecated.".format(k))
                if warn and k in self._experimental_options():
                    experimental_warn(
                        "The '{}' config key is experimental. "
                        "It might be changed or removed any time even without "
                        "prior notice.".format(k)
                    )
                setattr(self, k, v)
            elif k in self._deprecated_keys():
                k0 = self._get_new(k)
                if k0 in data_dict:
                    raise ConfigurationError(
                        "Cannot specify both '{}' and '{}' in config"
                        .format(k0, k)
                    )
                if warn:
                    deprecation_warn(
                        "The '{}' config key is deprecated, please use '{}' "
                        "instead".format(k, k0)
                    )
                if k in self._deprecated_aliases():
                    set_attr(k0, v)
                else:  # k in self._deprecated_alternatives:
                    _, converter = self._deprecated_alternatives()[k]
                    converter(self, v)
            else:
                raise AttributeError(k)

        rejected_keys = []
        for key, value in data_dict.items():
            if value is not None:
                try:
                    set_attr(key, value)
                except AttributeError as exc:
                    if not exc.args == (key,):
                        raise
                    rejected_keys.append(key)

        if rejected_keys:
            raise ConfigurationError("Unexpected config keys: "
                                     + ", ".join(rejected_keys))

    def __init__(self, *args, **kwargs):
        for arg in args:
            if isinstance(arg, Config):
                self.__update(arg, warn=False)
            else:
                self.__update(arg)
        self.__update(kwargs)

    def __repr__(self):
        attrs = []
        for key in self:
            attrs.append(" %s=%r" % (key, getattr(self, key)))
        return "<%s%s>" % (self.__class__.__name__, "".join(attrs))

    def __len__(self):
        return len(self.keys())

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.keys())


def _trust_to_trusted_certificates(pool_config, trust):
    if trust == TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
        pool_config.trusted_certificates = TrustSystemCAs()
    elif trust == TRUST_ALL_CERTIFICATES:
        pool_config.trusted_certificates = TrustAll()


class PoolConfig(Config):
    """ Connection pool configuration.
    """

    #: Max Connection Lifetime
    max_connection_lifetime = 3600  # seconds
    # The maximum duration the driver will keep a connection for before being removed from the pool.

    #: Max Connection Pool Size
    max_connection_pool_size = 100
    # The maximum total number of connections allowed, per host (i.e. cluster nodes), to be managed by the connection pool.

    #: Connection Timeout
    connection_timeout = 30.0  # seconds
    # The maximum amount of time to wait for a TCP connection to be established.

    #: Trust
    trust = DeprecatedAlternative(
        "trusted_certificates", _trust_to_trusted_certificates
    )
    # Specify how to determine the authenticity of encryption certificates provided by the Neo4j instance on connection.

    #: Custom Resolver
    resolver = None
    # Custom resolver function, returning list of resolved addresses.

    #: Encrypted
    encrypted = False
    # Specify whether to use an encrypted connection between the driver and server.

    #: SSL Certificates to Trust
    trusted_certificates = TrustSystemCAs()
    # Specify how to determine the authenticity of encryption certificates
    # provided by the Neo4j instance on connection.
    # * `neo4j.TrustSystemCAs()`: Use system trust store. (default)
    # * `neo4j.TrustAll()`: Trust any certificate.
    # * `neo4j.TrustCustomCAs("<path>", ...)`:
    #       Trust the specified certificate(s).

    #: Custom SSL context to use for wrapping sockets
    ssl_context = None
    # Use any custom SSL context to wrap sockets.
    # Overwrites `trusted_certificates` and `encrypted`.
    # The use of this option is strongly discouraged.

    #: User Agent (Python Driver Specific)
    user_agent = None
    # Specify the client agent name.

    #: Socket Keep Alive (Python and .NET Driver Specific)
    keep_alive = True
    # Specify whether TCP keep-alive should be enabled.

    #: Authentication provider
    auth = None

    #: Lowest notification severity for the server to return
    notifications_min_severity = None

    #: List of notification categories for the server to ignore
    notifications_disabled_categories = None

    #: Opt-Out of telemetry collection
    telemetry_disabled = False

    def get_ssl_context(self):
        if self.ssl_context is not None:
            return self.ssl_context

        if not self.encrypted:
            return None

        import ssl

        # SSL stands for Secure Sockets Layer and was originally created by Netscape.
        # SSLv2 and SSLv3 are the 2 versions of this protocol (SSLv1 was never publicly released).
        # After SSLv3, SSL was renamed to TLS.
        # TLS stands for Transport Layer Security and started with TLSv1.0 which is an upgraded version of SSLv3.
        # SSLv2 - (Disabled)
        # SSLv3 - (Disabled)
        # TLS 1.0 - Released in 1999, published as RFC 2246. (Disabled)
        # TLS 1.1 - Released in 2006, published as RFC 4346. (Disabled)
        # TLS 1.2 - Released in 2008, published as RFC 5246.
        # https://docs.python.org/3.7/library/ssl.html#ssl.PROTOCOL_TLS_CLIENT
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        # For recommended security options see
        # https://docs.python.org/3.10/library/ssl.html#protocol-versions
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

        if isinstance(self.trusted_certificates, TrustAll):
            # trust any certificate
            ssl_context.check_hostname = False
            # https://docs.python.org/3.7/library/ssl.html#ssl.CERT_NONE
            ssl_context.verify_mode = ssl.CERT_NONE
        elif isinstance(self.trusted_certificates, TrustCustomCAs):
            # trust the specified certificate(s)
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            for cert in self.trusted_certificates.certs:
                ssl_context.load_verify_locations(cert)
        else:
            # default
            # trust system CA certificates
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            # Must be load_default_certs, not set_default_verify_paths to
            # work on Windows with system CAs.
            ssl_context.load_default_certs()

        return ssl_context


class WorkspaceConfig(Config):
    """ WorkSpace configuration.
    """

    #: Connection Acquisition Timeout
    connection_acquisition_timeout = 60.0  # seconds
    # The maximum amount of time a session will wait when requesting a connection from the connection pool.
    # Since the process of acquiring a connection may involve creating a new connection, ensure that the value
    # of this configuration is higher than the configured Connection Timeout.

    #: Max Transaction Retry Time
    max_transaction_retry_time = 30.0  # seconds
    # The maximum amount of time that a managed transaction will retry before failing.

    #: Initial Retry Delay
    initial_retry_delay = 1.0  # seconds

    #: Retry Delay Multiplier
    retry_delay_multiplier = 2.0  # seconds

    #: Retry Delay Jitter Factor
    retry_delay_jitter_factor = 0.2  # seconds

    #: Database Name
    database = DEFAULT_DATABASE
    # Name of the database to query.
    # Note: The default database can be set on the Neo4j instance settings.

    #: Fetch Size
    fetch_size = 1000

    #: User to impersonate
    impersonated_user = None
    # Note that you need appropriate permissions to do so.

    #: Bookmark Manager
    bookmark_manager = None
    # Specify the bookmark manager to be used for sessions by default.


class SessionConfig(WorkspaceConfig):
    """ Session configuration.
    """

    #: Bookmarks
    bookmarks = None

    #: Default AccessMode
    default_access_mode = WRITE_ACCESS

    #: Auth token to temporarily switch the user
    auth = None

    #: Lowest notification severity for the server to return
    notifications_min_severity = None

    #: List of notification categories for the server to ignore
    notifications_disabled_categories = None


class TransactionConfig(Config):
    """ Transaction configuration. This is internal for now.

    neo4j.session.begin_transaction
    neo4j.Query
    neo4j.unit_of_work

    are both using the same settings.
    """
    #: Metadata
    metadata = None  # dictionary

    #: Timeout
    timeout = None  # seconds


class RoutingConfig(Config):
    """ Neo4jDriver routing settings. This is internal for now.
    """

    #: Routing Table Purge_Delay
    routing_table_purge_delay = 30.0  # seconds
    # The TTL + routing_table_purge_delay should be used to check if the database routing table should be removed.

    #: Max Routing Failures
    # max_routing_failures = 1

    #: Retry Timeout Delay
    # retry_timeout_delay = 5.0  # seconds
