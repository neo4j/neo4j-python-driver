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
]


class NotificationFilter(str, Enum):
    """A filter criterion for which notifications the server should return.

    Inherits from :class:`str` and :class:`Enum`. Hence, every driver API
    accepting a :class:`.NotificationFilter` value will also accept a string::

        True
        >>> NONE == "NONE"
        True
        >>> DEFAULT == "DEFAULT"
        True
        >>> ALL_ALL == "*.*"
        True
        >>> ALL_QUERY == "*.QUERY"
        True
        >>> WARNING_ALL == "WARNING.*"
        True
        >>> WARNING_DEPRECATION == "WARNING.DEPRECATION"
        True
        >>> WARNING_HINT == "WARNING.HINT"
        True
        >>> WARNING_QUERY == "WARNING.QUERY"
        True
        >>> WARNING_UNSUPPORTED == "WARNING.UNSUPPORTED"
        True
        >>> INFORMATION_ALL == "INFORMATION.*"
        True
        >>> INFORMATION_RUNTIME == "INFORMATION.RUNTIME"
        True
        >>> INFORMATION_QUERY == "INFORMATION.QUERY"
        True
        >>> INFORMATION_PERFORMANCE == "INFORMATION.PERFORMANCE"
        True

    :attr:`NONE` instructs the server to not send any notifications.
    :attr:`DEFAULT` leaves it up to the server which notifications to send.

    The other choices are a combination of a severity and a category.

    .. versionadded:: 5.2

    .. seealso::
        :ref:`driver-configuration-ref`, :ref:`session-configuration-ref`
    """
    NONE = "NONE"
    DEFAULT = "DEFAULT"
    ALL_ALL = "*.*"
    ALL_QUERY = "*.QUERY"
    WARNING_ALL = "WARNING.*"
    WARNING_DEPRECATION = "WARNING.DEPRECATION"
    WARNING_HINT = "WARNING.HINT"
    WARNING_QUERY = "WARNING.QUERY"
    WARNING_UNSUPPORTED = "WARNING.UNSUPPORTED"
    INFORMATION_ALL = "INFORMATION.*"
    INFORMATION_RUNTIME = "INFORMATION.RUNTIME"
    INFORMATION_QUERY = "INFORMATION.QUERY"
    INFORMATION_PERFORMANCE = "INFORMATION.PERFORMANCE"


if t.TYPE_CHECKING:
    T_NotificationFilter = t.Union[
        NotificationFilter,
        te.Literal[
            "NONE",
            "DEFAULT",
            "*.*",
            "*.QUERY",
            "WARNING.*",
            "WARNING.DEPRECATION",
            "WARNING.HINT",
            "WARNING.QUERY",
            "WARNING.UNSUPPORTED",
            "INFORMATION.*",
            "INFORMATION.RUNTIME",
            "INFORMATION.QUERY",
            "INFORMATION.PERFORMANCE",
        ],
    ]


class NotificationSeverity(Enum):
    """Server-side notification severity.

    .. versionadded:: 5.2

    .. seealso:: :class:`SummaryNotification.severity_level`
    """

    WARNING = "WARNING"
    INFORMATION = "INFORMATION"
    #: Used when server provides a Severity which the driver is unaware of.
    UNKNOWN = "UNKNOWN"


class NotificationCategory(Enum):
    """Server-side notification category.

    .. versionadded:: 5.2

    .. seealso:: :class:`SummaryNotification.category`
    """

    HINT = "HINT"
    QUERY = "QUERY"
    UNSUPPORTED = "UNSUPPORTED"
    PERFORMANCE = "PERFORMANCE"
    DEPRECATION = "DEPRECATION"
    RUNTIME = "RUNTIME"
    #: Used when server provides a Category which the driver is unaware of.
    UNKNOWN = "UNKNOWN"
