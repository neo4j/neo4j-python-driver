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

import typing as t
from enum import Enum


if t.TYPE_CHECKING:
    import typing_extensions as te


__all__ = [
    "NotificationCategory",
    "NotificationFilter",
    "NotificationSeverity",
    "RoutingControl",
]


class NotificationFilter(str, Enum):
    """A filter criterion for which notifications the server should return.

    Inherits from :class:`str` and :class:`Enum`. Hence, every driver API
    accepting a :class:`.NotificationFilter` value will also accept a string::

        >>> ALL_ALL == "*.*"
        True
        >>> WARNING_ALL == "WARNING.*"
        True
        >>> ALL_DEPRECATION == "*.DEPRECATION"
        True
        >>> INFORMATION_HINT == "INFORMATION.HINT"
        True

    When connected to a server version 5.? or older, configuring anything other
    than :meth:`.server_default` will result in an :exc:`.ConfigurationError`.

    When connected to an older server version, and choosing a filter that is
    not supported by that server version, the server will ignore that filter
    and return a notification of type `WARNING.UNSUPPORTED` which cannot be
    suppressed by any filters.

    .. versionadded:: 5.?

    .. seealso::
        :ref:`driver-configuration-ref`, :ref:`session-configuration-ref`
    """

    ALL_ALL = "*.*"
    WARNING_ALL = "WARNING.*"
    WARNING_DEPRECATION = "WARNING.DEPRECATION"
    WARNING_HINT = "WARNING.HINT"
    WARNING_UNRECOGNIZED = "WARNING.UNRECOGNIZED"
    WARNING_UNSUPPORTED = "WARNING.UNSUPPORTED"
    WARNING_GENERIC = "WARNING.GENERIC"
    WARNING_PERFORMANCE = "WARNING.PERFORMANCE"
    INFORMATION_ALL = "INFORMATION.*"
    INFORMATION_DEPRECATION = "INFORMATION.DEPRECATION"
    INFORMATION_HINT = "INFORMATION.HINT"
    INFORMATION_UNRECOGNIZED = "INFORMATION.UNRECOGNIZED"
    INFORMATION_UNSUPPORTED = "INFORMATION.UNSUPPORTED"
    INFORMATION_GENERIC = "INFORMATION.GENERIC"
    INFORMATION_PERFORMANCE = "INFORMATION.PERFORMANCE"
    ALL_DEPRECATION = "*.DEPRECATION"
    ALL_HINT = "*.HINT"
    ALL_UNRECOGNIZED = "*.UNRECOGNIZED"
    ALL_UNSUPPORTED = "*.UNSUPPORTED"
    ALL_GENERIC = "*.GENERIC"
    ALL_PERFORMANCE = "*.PERFORMANCE"

    @staticmethod
    def none() -> t.List[NotificationFilter]:
        """Value to disable all notifications.

            >>> NotificationFilter.none() == []
            True

        Example::

            driver = neo4j.GraphDatabase.driver(
                uri, auth=auth,
                notification_filters=neo4j.NotificationFilter.none()
            )
        """

        return []

    @staticmethod
    def server_default() -> None:
        """Value to let the server choose which notifications to send.

            >>> NotificationFilter.server_default() is None
            True

        Example::

            driver = neo4j.GraphDatabase.driver(
                uri, auth=auth,
                notification_filters=neo4j.NotificationFilter.server_default()
            )
        """
        return None

    def __str__(self):
        return self.value


if t.TYPE_CHECKING:
    T_NotificationFilter = t.Union[
        NotificationFilter,
        te.Literal[
            "*.*",
            "WARNING.*",
            "WARNING.DEPRECATION",
            "WARNING.HINT",
            "WARNING.UNRECOGNIZED",
            "WARNING.UNSUPPORTED",
            "WARNING.GENERIC",
            "WARNING.PERFORMANCE",
            "INFORMATION.*",
            "INFORMATION.DEPRECATION",
            "INFORMATION.HINT",
            "INFORMATION.UNRECOGNIZED",
            "INFORMATION.UNSUPPORTED",
            "INFORMATION.GENERIC",
            "INFORMATION.PERFORMANCE",
            "*.DEPRECATION",
            "*.HINT",
            "*.UNRECOGNIZED",
            "*.UNSUPPORTED",
            "*.GENERIC",
            "*.PERFORMANCE",
        ],
    ]


class NotificationSeverity(str, Enum):
    """Server-side notification severity.

    Inherits from :class:`str` and :class:`Enum`. Hence, can also be compared
    to its string value::

        >>> WARNING == "WARNING"
        True
        >>> INFORMATION == "INFORMATION"
        True
        >>> UNKNOWN == "UNKNOWN"
        True

    Example::

        import logging

        from neo4j import NotificationSeverity

        log = logging.getLogger(__name__)

        ...

        summary = session.run("RETURN 1").consume()

        for notification in summary.summary_notifications:
            sevirity = notification.severity_level
            if severity == NotificationSeverity.WARNING:
                # or severity_level == "WARNING"
                log.warning("%r", notification)
            elif severity == NotificationSeverity.INFORMATION:
                # or severity_level == "INFORMATION"
                log.info("%r", notification)
            else:
                # assert severity == NotificationSeverity.UNKNOWN
                # or severity_level == "UNKNOWN"
                log.debug("%r", notification)

    .. versionadded:: 5.?

    .. seealso:: :attr:`SummaryNotification.severity_level`
    """

    WARNING = "WARNING"
    INFORMATION = "INFORMATION"
    #: Used when the server provides a Severity which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver.
    UNKNOWN = "UNKNOWN"


class NotificationCategory(str, Enum):
    """Server-side notification category.

    Inherits from :class:`str` and :class:`Enum`. Hence, can also be compared
    to its string value::

        >>> DEPRECATION == "DEPRECATION"
        True
        >>> GENERIC == "GENERIC"
        True
        >>> UNKNOWN == "UNKNOWN"
        True

    .. versionadded:: 5.?

    .. seealso:: :attr:`SummaryNotification.category`
    """

    HINT = "HINT"
    UNRECOGNIZED = "UNRECOGNIZED"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    GENERIC = "GENERIC"
    #: Used when the server provides a Category which the driver is unaware of.
    #: This can happen when connecting to a server newer than the driver.
    UNKNOWN = "UNKNOWN"


class RoutingControl(str, Enum):
    """Selection which cluster members to route a query connect to.

    Inherits from :class:`str` and :class:`Enum`. Hence, every driver API
    accepting a :class:`.RoutingControl` value will also accept a string

        >>> RoutingControl.READERS == "r"
        True
        >>> RoutingControl.WRITERS == "w"
        True

    **This is experimental.**
    It might be changed or removed any time even without prior notice.

    .. seealso::
        :attr:`.AsyncDriver.execute_query`, :attr:`.Driver.execute_query`

    .. versionadded:: 5.5
    """
    READERS = "r"
    WRITERS = "w"


if t.TYPE_CHECKING:
    T_RoutingControl = t.Union[
        RoutingControl,
        te.Literal["r", "w"],
    ]
