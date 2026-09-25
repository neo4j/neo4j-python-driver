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


from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from urllib.parse import (
    parse_qs,
    urlparse,
)

from . import (
    _typing as t,
    api,
)
from ._warnings import (
    deprecation_warn,
    preview_warn,
)
from .exceptions import ConfigurationError


if t.TYPE_CHECKING:
    from urllib.parse import ParseResult


__all__ = [
    "DRIVER_BOLT",
    "DRIVER_NEO4J",
    "SECURITY_TYPE_NOT_SECURE",
    "SECURITY_TYPE_SECURE",
    "SECURITY_TYPE_SELF_SIGNED_CERTIFICATE",
    "NotificationCategory",
    "NotificationClassification",
    "NotificationDisabledCategory",
    "NotificationDisabledClassification",
    "NotificationMinimumSeverity",
    "NotificationSeverity",
    "RoutingControl",
    "TelemetryAPI",
    "check_access_mode",
    "parse_neo4j_uri",
    "parse_routing_context",
]


DRIVER_BOLT: t.Final[t.Literal["DRIVER_BOLT"]] = "DRIVER_BOLT"
DRIVER_NEO4J: t.Final[t.Literal["DRIVER_NEO4J"]] = "DRIVER_NEO4J"
DRIVER_HTTP: t.Final[t.Literal["DRIVER_HTTP"]] = "DRIVER_HTTP"

if t.TYPE_CHECKING:
    T_DRIVER_TYPE = t.Literal["DRIVER_BOLT", "DRIVER_NEO4J", "DRIVER_HTTP"]

SECURITY_TYPE_NOT_SECURE: t.Final[str] = "SECURITY_TYPE_NOT_SECURE"
SECURITY_TYPE_SELF_SIGNED_CERTIFICATE: t.Final[str] = (
    "SECURITY_TYPE_SELF_SIGNED_CERTIFICATE"
)
SECURITY_TYPE_SECURE: t.Final[str] = "SECURITY_TYPE_SECURE"


def parse_neo4j_uri(uri: str) -> tuple[T_DRIVER_TYPE, str, ParseResult]:
    parsed = urlparse(uri)

    driver_type: T_DRIVER_TYPE
    security_type: str

    if parsed.scheme == api.URI_SCHEME_NEO4J:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_NOT_SECURE
    elif parsed.scheme == api.URI_SCHEME_NEO4J_SECURE:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_SECURE
    elif parsed.scheme == api.URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE:
        driver_type = DRIVER_NEO4J
        security_type = SECURITY_TYPE_SELF_SIGNED_CERTIFICATE
    elif parsed.scheme == api.URI_SCHEME_BOLT:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_NOT_SECURE
    elif parsed.scheme == api.URI_SCHEME_BOLT_SECURE:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_SECURE
    elif parsed.scheme == api.URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE:
        driver_type = DRIVER_BOLT
        security_type = SECURITY_TYPE_SELF_SIGNED_CERTIFICATE
    elif parsed.scheme == api.URI_SCHEME_HTTP:
        driver_type = DRIVER_HTTP
        security_type = SECURITY_TYPE_NOT_SECURE
    elif parsed.scheme == api.URI_SCHEME_HTTPS:
        driver_type = DRIVER_HTTP
        security_type = SECURITY_TYPE_SECURE
    elif parsed.scheme == api.URI_SCHEME_BOLT_ROUTING:
        raise ConfigurationError(
            f"Uri scheme {parsed.scheme!r} has been renamed. "
            f"Use {api.URI_SCHEME_NEO4J!r}"
        )
    else:
        supported_schemes = [
            api.URI_SCHEME_BOLT,
            api.URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
            api.URI_SCHEME_BOLT_SECURE,
            api.URI_SCHEME_NEO4J,
            api.URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
            api.URI_SCHEME_NEO4J_SECURE,
            api.URI_SCHEME_HTTP,
            api.URI_SCHEME_HTTPS,
        ]
        raise ConfigurationError(
            f"URI scheme {parsed.scheme!r} is not supported. "
            f"Supported URI schemes are {supported_schemes}. "
            "Examples: bolt://host[:port] or "
            "neo4j://host[:port][?routing_context]"
        )

    if driver_type == DRIVER_BOLT:
        _UriVerifier.bolt_verifier(parsed, uri).verify()
    elif driver_type == DRIVER_NEO4J:
        _UriVerifier.neo4j_verifier(parsed, uri).verify()
    elif driver_type == DRIVER_HTTP:
        preview_warn(
            (
                "The Query API/HTTP support in the Neo4j Python "
                "driver is currently in preview."
            ),
            stack_level=3,
        )
        _UriVerifier.http_verifier(parsed, uri).verify()
    else:
        t.assert_never(driver_type)

    return driver_type, security_type, parsed


@dataclass(frozen=True)
class _UriVerifier:
    _parsed: ParseResult
    _driver_type: str
    _scheme: str
    _uri: str
    _block_userinfo: bool = True
    _block_path: bool = False
    _block_fragment: bool = False
    _block_query: bool = False
    _hard_block: bool = False  # TODO: 7.0 - remove and always raise

    @classmethod
    def bolt_verifier(cls, parsed: ParseResult, uri: str) -> t.Self:
        return cls(
            _parsed=parsed,
            _driver_type="direct",
            _scheme="bolt[+s[sc]]://",
            _uri=uri,
            _block_path=True,
            _block_fragment=True,
            _block_query=True,
        )

    @classmethod
    def neo4j_verifier(cls, parsed: ParseResult, uri: str) -> t.Self:
        return cls(
            _parsed=parsed,
            _driver_type="routing",
            _scheme="neo4j[+s[sc]]://",
            _uri=uri,
            _block_path=True,
            _block_fragment=True,
        )

    @classmethod
    def http_verifier(cls, parsed: ParseResult, uri: str) -> t.Self:
        return cls(
            _parsed=parsed,
            _driver_type="Query API/HTTP",
            _scheme="http[s]://",
            _uri=uri,
            _block_fragment=True,
            _block_query=True,
            _hard_block=True,
        )

    def verify(self) -> None:
        if self._block_userinfo:
            self._block_uri_userinfo()
        if self._block_path:
            self._block_uri_path()
        if self._block_fragment:
            self._block_uri_fragment()
        if self._block_query:
            self._block_uri_query()

    def _block_uri_userinfo(self) -> None:
        if self._parsed.password or self._parsed.username:
            raise ConfigurationError(
                f"URI userinfo is not supported by {self._driver_type} "
                f'drivers ("{self._scheme}" scheme). '
                f"Given URI: {self._uri!r}."
            )

    def _block_uri_path(self) -> None:
        if self._parsed.path not in {"", "/"}:
            # TODO: 7.0 - always raise
            if not self._hard_block:
                deprecation_warn(
                    f"Creating {self._driver_type} drivers with "
                    f'("{self._scheme}" scheme) with URI path is '
                    "deprecated. The path will be ignored. "
                    "This will raise an error in a future release. "
                    f"Given URI: {self._uri!r}",
                    stack_level=4,
                )
                return
            raise ConfigurationError(
                f"URI path is not supported by {self._driver_type} "
                f'drivers ("{self._scheme}" scheme). '
                f"Given URI: {self._uri!r}."
            )

    def _block_uri_fragment(self) -> None:
        if self._parsed.fragment:
            # TODO: 7.0 - always raise
            if not self._hard_block:
                deprecation_warn(
                    f"Creating {self._driver_type} drivers with "
                    f'("{self._scheme}" scheme) with URI fragment is '
                    "deprecated. The fragments will be ignored. "
                    "This will raise an error in a future release. "
                    f"Given URI: {self._uri!r}",
                    stack_level=4,
                )
                return
            raise ConfigurationError(
                f"URI fragment is not supported by {self._driver_type} "
                f'drivers ("{self._scheme}" scheme). '
                f"Given URI: {self._uri!r}."
            )

    def _block_uri_query(self) -> None:
        if parse_routing_context(self._parsed.query):
            raise ConfigurationError(
                "Routing context (providing URI query parameters) is not "
                f"supported by {self._driver_type} drivers "
                f'("{self._scheme}" scheme). Given URI: {self._uri!r}.'
            )


def check_access_mode(access_mode):
    if access_mode not in {api.READ_ACCESS, api.WRITE_ACCESS}:
        raise ValueError(
            f"Unsupported access mode {access_mode}, must be one of "
            f"'{api.READ_ACCESS}' or '{api.WRITE_ACCESS}'."
        )

    return access_mode


def parse_routing_context(query):
    """
    Parse the query portion of a URI.

    Generates a routing context dictionary.
    """
    if not query:
        return {}

    context = {}
    parameters = parse_qs(query, True)
    for key in parameters:
        value_list = parameters[key]
        if len(value_list) != 1:
            raise ConfigurationError(
                f"Duplicated query parameters with key '{key}', value "
                f"'{value_list}' found in query string '{query}'"
            )
        value = value_list[0]
        if not value:
            raise ConfigurationError(
                f"Invalid parameters:'{key}={value}' in query string "
                f"'{query}'."
            )
        context[key] = value

    return context


class NotificationMinimumSeverity(str, Enum):
    """
    Filter notifications returned by the server by minimum severity.

    For GQL-aware servers, notifications are a subset of GqlStatusObjects.
    See also :attr:`.GqlStatusObject.is_notification`.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.NotificationMinimumSeverity` value
    will also accept a string::

        >>> NotificationMinimumSeverity.OFF == "OFF"
        True
        >>> NotificationMinimumSeverity.WARNING == "WARNING"
        True
        >>> NotificationMinimumSeverity.INFORMATION == "INFORMATION"
        True

    .. seealso::
        driver config :ref:`driver-notifications-min-severity-ref`,
        session config :ref:`session-notifications-min-severity-ref`

    .. versionadded:: 5.7
    """

    OFF = "OFF"
    WARNING = "WARNING"
    INFORMATION = "INFORMATION"


if t.TYPE_CHECKING:
    T_NotificationMinimumSeverity = (
        NotificationMinimumSeverity
        | t.Literal[
            "OFF",
            "WARNING",
            "INFORMATION",
        ]
    )
    __all__.append("T_NotificationMinimumSeverity")


class NotificationSeverity(str, Enum):
    """
    Server-side notification severity.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> NotificationSeverity.WARNING == "WARNING"
        True
        >>> NotificationSeverity.INFORMATION == "INFORMATION"
        True
        >>> NotificationSeverity.UNKNOWN == "UNKNOWN"
        True

    Example::

        import logging

        from neo4j import NotificationSeverity


        log = logging.getLogger(__name__)

        ...

        summary = session.run("RETURN 1").consume()

        for notification in summary.summary_notifications:
            severity = notification.severity_level
            if severity == NotificationSeverity.WARNING:
                # or severity == "WARNING"
                log.warning("%r", notification)
            elif severity == NotificationSeverity.INFORMATION:
                # or severity == "INFORMATION"
                log.info("%r", notification)
            else:
                # assert severity == NotificationSeverity.UNKNOWN
                # or severity == "UNKNOWN"
                log.debug("%r", notification)

    .. seealso:: :attr:`.SummaryNotification.severity_level`

    .. versionadded:: 5.7
    """

    WARNING = "WARNING"
    INFORMATION = "INFORMATION"
    #: Used when the server provides a Severity which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver.
    UNKNOWN = "UNKNOWN"


class NotificationDisabledCategory(str, Enum):
    """
    Filter notifications returned by the server by category.

    For GQL-aware servers, notifications are a subset of GqlStatusObjects.
    See also :attr:`.GqlStatusObject.is_notification`.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.NotificationDisabledCategory` value
    will also accept a string::

        >>> NotificationDisabledCategory.UNRECOGNIZED == "UNRECOGNIZED"
        True
        >>> NotificationDisabledCategory.PERFORMANCE == "PERFORMANCE"
        True
        >>> NotificationDisabledCategory.DEPRECATION == "DEPRECATION"
        True

    .. seealso::
        driver config :ref:`driver-notifications-disabled-categories-ref`,
        session config :ref:`session-notifications-disabled-categories-ref`

    .. versionadded:: 5.7

    .. versionchanged:: 5.14
        Added categories :attr:`.SECURITY` and :attr:`.TOPOLOGY`.

    .. versionchanged:: 5.24
        Added category :attr:`.SCHEMA`.

    .. deprecated:: 6.0
        Use :class:`.NotificationDisabledClassification` instead.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    #: Requires server version 5.13 or newer.
    TOPOLOGY = "TOPOLOGY"
    #: Requires server version 5.17 or newer.
    SCHEMA = "SCHEMA"


class NotificationDisabledClassification(str, Enum):
    """
    Identical to :class:`.NotificationDisabledCategory`.

    This alternative is provided for a consistent naming with
    :attr:`.GqlStatusObject.classification`.

    .. seealso::
        driver config
        :ref:`driver-notifications-disabled-classifications-ref`,
        session config
        :ref:`session-notifications-disabled-classifications-ref`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    #: Requires server version 5.13 or newer.
    TOPOLOGY = "TOPOLOGY"
    #: Requires server version 5.17 or newer.
    SCHEMA = "SCHEMA"


if t.TYPE_CHECKING:
    T_NotificationDisabledClassification = (
        NotificationDisabledCategory
        | NotificationDisabledClassification
        | t.Literal[
            "HINT",
            "UNRECOGNIZED",
            "UNSUPPORTED",
            "PERFORMANCE",
            "DEPRECATION",
            "GENERIC",
            "SECURITY",
            "TOPOLOGY",
            "SCHEMA",
        ]
    )
    __all__.append("T_NotificationDisabledClassification")


class NotificationCategory(str, Enum):
    """
    Server-side notification category.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> NotificationCategory.DEPRECATION == "DEPRECATION"
        True
        >>> NotificationCategory.GENERIC == "GENERIC"
        True
        >>> NotificationCategory.UNKNOWN == "UNKNOWN"
        True

    .. seealso:: :attr:`.SummaryNotification.category`

    .. versionadded:: 5.7

    .. versionchanged:: 5.14
        Added categories :attr:`.SECURITY` and :attr:`.TOPOLOGY`.

    .. versionchanged:: 5.24
        Added category :attr:`.SCHEMA`.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    TOPOLOGY = "TOPOLOGY"
    SCHEMA = "SCHEMA"
    #: Used when the server provides a Category which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before notification categories were introduced.
    UNKNOWN = "UNKNOWN"


class NotificationClassification(str, Enum):
    """
    Identical to :class:`.NotificationCategory`.

    This alternative is provided for a consistent naming with
    :attr:`.GqlStatusObject.classification`.

    .. seealso:: :attr:`.GqlStatusObject.classification`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.

    .. versionchanged:: 6.0 Stabilized from preview.
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    SECURITY = "SECURITY"
    TOPOLOGY = "TOPOLOGY"
    SCHEMA = "SCHEMA"
    #: Used when the server provides a Category which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before notification categories were introduced.
    UNKNOWN = "UNKNOWN"


class RoutingControl(str, Enum):
    """
    Selection which cluster members to route a query connect to.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.RoutingControl` value will also
    accept a string::

        >>> RoutingControl.READ == "r"
        True
        >>> RoutingControl.WRITE == "w"
        True

    .. seealso::
        :meth:`.AsyncDriver.execute_query`, :meth:`.Driver.execute_query`

    .. versionadded:: 5.5

    .. versionchanged:: 5.8

        * Renamed ``READERS`` to ``READ`` and ``WRITERS`` to ``WRITE``.
        * Stabilized from experimental.
    """

    READ = "r"
    WRITE = "w"


class TelemetryAPI(int, Enum):
    TX_FUNC = 0
    TX = 1
    AUTO_COMMIT = 2
    DRIVER = 3


if t.TYPE_CHECKING:
    T_RoutingControl = RoutingControl | t.Literal["r", "w"]
    __all__.append("T_RoutingControl")
