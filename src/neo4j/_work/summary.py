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
from dataclasses import dataclass


if t.TYPE_CHECKING:
    import typing_extensions as te

from .._api import (
    NotificationCategory,
    NotificationSeverity,
)
from .._exceptions import BoltProtocolError
from ..addressing import Address
from ..api import ServerInfo


# TODO: This logic should be inside the Bolt subclasses, because it can change depending on Bolt Protocol Version.


class ResultSummary:
    """ A summary of execution returned with a :class:`.Result` object.
    """

    #: A :class:`neo4j.ServerInfo` instance. Provides some basic information of the server where the result is obtained from.
    server: ServerInfo

    #: The database name where this summary is obtained from.
    database: t.Optional[str]

    #: The query that was executed to produce this result.
    query: t.Optional[str]

    #: Dictionary of parameters passed with the statement.
    parameters: t.Optional[t.Dict[str, t.Any]]

    #: A string that describes the type of query
    # ``'r'`` = read-only, ``'rw'`` = read/write, ``'w'`` = write-only,
    # ``'s'`` = schema.
    query_type: t.Union[te.Literal["r", "rw", "w", "s"], None]

    #: A :class:`neo4j.SummaryCounters` instance. Counters for operations the query triggered.
    counters: SummaryCounters

    #: Dictionary that describes how the database will execute the query.
    plan: t.Optional[dict]

    #: Dictionary that describes how the database executed the query.
    profile: t.Optional[dict]

    #: The time it took for the server to have the result available. (milliseconds)
    result_available_after: t.Optional[int]

    #: The time it took for the server to consume the result. (milliseconds)
    result_consumed_after: t.Optional[int]

    #: A list of Dictionaries containing notification information.
    #: Notifications provide extra information for a user executing a statement.
    #: They can be warnings about problematic queries or other valuable information that can be
    #: presented in a client.
    #: Unlike failures or errors, notifications do not affect the execution of a statement.
    #:
    #: .. seealso:: :attr:`.summary_notifications`
    notifications: t.Optional[t.List[dict]]

    #: The same as ``notifications`` but in a parsed, structured form.
    _summary_notifications: t.List[SummaryNotification]

    def __init__(self, address: Address, **metadata: t.Any) -> None:
        self.metadata = metadata
        self.server = metadata["server"]
        self.database = metadata.get("db")
        self.query = metadata.get("query")
        self.parameters = metadata.get("parameters")
        if "type" in metadata:
            self.query_type = metadata["type"]
            if self.query_type not in ["r", "w", "rw", "s"]:
                raise BoltProtocolError(
                    "Unexpected query type '%s' received from server. Consider "
                    "updating the driver.", address
                )
        self.query_type = metadata.get("type")
        self.plan = metadata.get("plan")
        self.profile = metadata.get("profile")
        self.notifications = metadata.get("notifications")
        self.counters = SummaryCounters(metadata.get("stats", {}))
        if self.server.protocol_version[0] < 3:
            self.result_available_after = metadata.get("result_available_after")
            self.result_consumed_after = metadata.get("result_consumed_after")
        else:
            self.result_available_after = metadata.get("t_first")
            self.result_consumed_after = metadata.get("t_last")

    @property
    def summary_notifications(self) -> t.List[SummaryNotification]:
        """The same as ``notifications`` but in a parsed, structured form.

        .. versionadded:: 5.7

        .. seealso:: :attr:`.notifications`, :class:`.SummaryNotification`
        """
        if getattr(self, "_summary_notifications", None) is None:
            self._summary_notifications = [
                SummaryNotification._from_metadata(n)
                for n in self.notifications or ()
            ]
        return self._summary_notifications


class SummaryCounters:
    """ Contains counters for various operations that a query triggered.
    """

    #:
    nodes_created: int = 0

    #:
    nodes_deleted: int = 0

    #:
    relationships_created: int = 0

    #:
    relationships_deleted: int = 0

    #:
    properties_set: int = 0

    #:
    labels_added: int = 0

    #:
    labels_removed: int = 0

    #:
    indexes_added: int = 0

    #:
    indexes_removed: int = 0

    #:
    constraints_added: int = 0

    #:
    constraints_removed: int = 0

    #:
    system_updates: int = 0

    _contains_updates = None
    _contains_system_updates = None

    def __init__(self, statistics) -> None:
        key_to_attr_name = {
            "nodes-created": "nodes_created",
            "nodes-deleted": "nodes_deleted",
            "relationships-created": "relationships_created",
            "relationships-deleted": "relationships_deleted",
            "properties-set": "properties_set",
            "labels-added": "labels_added",
            "labels-removed": "labels_removed",
            "indexes-added": "indexes_added",
            "indexes-removed": "indexes_removed",
            "constraints-added": "constraints_added",
            "constraints-removed": "constraints_removed",
            "system-updates": "system_updates",
            "contains-updates": "_contains_updates",
            "contains-system-updates": "_contains_system_updates",
        }
        for key, value in dict(statistics).items():
            attr_name = key_to_attr_name.get(key)
            if attr_name:
                setattr(self, attr_name, value)

    def __repr__(self) -> str:
        return repr(vars(self))

    @property
    def contains_updates(self) -> bool:
        """True if any of the counters except for system_updates, are greater
        than 0. Otherwise False."""
        if self._contains_updates is not None:
            return self._contains_updates
        return bool(
            self.nodes_created or self.nodes_deleted
            or self.relationships_created or self.relationships_deleted
            or self.properties_set or self.labels_added
            or self.labels_removed or self.indexes_added
            or self.indexes_removed or self.constraints_added
            or self.constraints_removed
        )

    @property
    def contains_system_updates(self) -> bool:
        """True if the system database was updated, otherwise False."""
        if self._contains_system_updates is not None:
            return self._contains_system_updates
        return self.system_updates > 0


_SEVERITY_LOOKUP = {
   "WARNING": NotificationSeverity.WARNING,
   "INFORMATION": NotificationSeverity.INFORMATION,
}

_CATEGORY_LOOKUP = {
    "HINT": NotificationCategory.HINT,
    "UNRECOGNIZED": NotificationCategory.UNRECOGNIZED,
    "UNSUPPORTED": NotificationCategory.UNSUPPORTED,
    "PERFORMANCE": NotificationCategory.PERFORMANCE,
    "DEPRECATION": NotificationCategory.DEPRECATION,
    "GENERIC": NotificationCategory.GENERIC,
}


@dataclass
class SummaryNotification:
    """Structured form of a notification received from the server.

    .. versionadded:: 5.7

    .. seealso:: :attr:`.ResultSummary.summary_notifications`
    """

    title: str = ""
    code: str = ""
    description: str = ""
    severity_level: NotificationSeverity = NotificationSeverity.UNKNOWN
    category: NotificationCategory = NotificationCategory.UNKNOWN
    raw_severity_level: str = ""
    raw_category: str = ""
    position: t.Optional[SummaryNotificationPosition] = None

    @classmethod
    def _from_metadata(cls, metadata):
        if not isinstance(metadata, dict):
            return SummaryNotification()
        kwargs = {
            "position": SummaryNotificationPosition._from_metadata(metadata)
        }
        for key in ("title", "code", "description"):
            value = metadata.get(key)
            if isinstance(value, str):
                kwargs[key] = value
        severity = metadata.get("severity")
        if isinstance(severity, str):
            kwargs["raw_severity_level"] = severity
            kwargs["severity_level"] = _SEVERITY_LOOKUP.get(
                severity, NotificationSeverity.UNKNOWN
            )
        category = metadata.get("category")
        if isinstance(category, str):
            kwargs["raw_category"] = category
            kwargs["category"] = _CATEGORY_LOOKUP.get(
                category, NotificationCategory.UNKNOWN
            )
        return cls(**kwargs)


@dataclass
class SummaryNotificationPosition:
    """Structured form of a notification position received from the server.

    .. versionadded:: 5.7

    .. seealso:: :class:`.SummaryNotification`
    """

    #: The line number of the notification. Line numbers start at 1.
    # Defaults to -1 if the server's data could not be interpreted.
    line: int = -1
    #: The column number of the notification. Column numbers start at 1.
    # Defaults to -1 if the server's data could not be interpreted.
    column: int = -1
    #: The character offset of the notification. Offsets start at 0.
    # Defaults to -1 if the server's data could not be interpreted.
    offset: int = -1

    @classmethod
    def _from_metadata(cls, metadata):
        metadata = metadata.get("position")
        if not isinstance(metadata, dict):
            return None
        kwargs = {}
        for key in ("line", "column", "offset"):
            value = metadata.get(key)
            if isinstance(value, int):
                kwargs[key] = value
        return cls(**kwargs)
