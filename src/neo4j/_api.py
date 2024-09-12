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

import typing as t
from enum import Enum


if t.TYPE_CHECKING:
    import typing_extensions as te


__all__ = [
    "NotificationCategory",
    "NotificationClassification",
    "NotificationDisabledCategory",
    "NotificationDisabledClassification",
    "NotificationMinimumSeverity",
    "NotificationSeverity",
    "RoutingControl",
    "TelemetryAPI",
]


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
    T_NotificationMinimumSeverity = t.Union[
        NotificationMinimumSeverity,
        te.Literal[
            "OFF",
            "WARNING",
            "INFORMATION",
        ],
    ]
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

    **This is a preview**.
    It might be changed without following the deprecation policy.

    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso::
        driver config
        :ref:`driver-notifications-disabled-classifications-ref`,
        session config
        :ref:`session-notifications-disabled-classifications-ref`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.
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
    T_NotificationDisabledCategory = t.Union[
        NotificationDisabledCategory,
        NotificationDisabledClassification,
        te.Literal[
            "HINT",
            "UNRECOGNIZED",
            "UNSUPPORTED",
            "PERFORMANCE",
            "DEPRECATION",
            "GENERIC",
            "SECURITY",
            "TOPOLOGY",
            "SCHEMA",
        ],
    ]
    __all__.append("T_NotificationDisabledCategory")


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

    **This is a preview**.
    It might be changed without following the deprecation policy.

    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :attr:`.GqlStatusObject.classification`

    .. versionadded:: 5.22

    .. versionchanged:: 5.24
        Added classification :attr:`.SCHEMA`.
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
    T_RoutingControl = t.Union[
        RoutingControl,
        te.Literal["r", "w"],
    ]
    __all__.append("T_RoutingControl")
