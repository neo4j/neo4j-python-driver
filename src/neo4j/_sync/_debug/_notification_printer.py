from __future__ import annotations

import typing as t

from ..._work import SummaryNotification


class NotificationPrinter:
    notification: SummaryNotification
    query: t.Optional[str]

    def __init__(
        self,
        notification: SummaryNotification,
        query: t.Optional[str] = None
    ) -> None:
        self.notification = notification
        self.query = query

    def __str__(self):
        if self.query is None:
            return str(self.notification)
        pos = self.notification.position
        if pos is None:
            return f"{self.notification} for query:\n{self.query}"
        s = f"{self.notification} for query:\n"
        query_lines = self.query.splitlines()
        if pos.line > len(query_lines):
            return s + self.query
        query_lines = (
            query_lines[:pos.line]
            + [" " * (pos.column - 1) + "^"]
            + query_lines[pos.line:]
        )
        s += "\n".join(query_lines)
        return s
