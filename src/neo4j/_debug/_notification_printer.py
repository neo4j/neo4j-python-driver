from __future__ import annotations

import typing as t

from .._work import SummaryNotification


class NotificationPrinter:
    notification: SummaryNotification
    query: t.Optional[str]

    def __init__(
        self,
        notification: SummaryNotification,
        query: t.Optional[str] = None,
        one_line: bool = False,
    ) -> None:
        self.notification = notification
        self.query = query
        self._one_line = one_line

    def __str__(self):
        if self.query is None:
            return str(self.notification)
        if self._one_line:
            return f"{self.notification} for query: {self.query!r}"
        pos = self.notification.position
        if pos is None:
            return f"{self.notification} for query:\n{self.query}"
        s = f"{self.notification} for query:\n"
        query_lines = self.query.splitlines()
        if pos.line <= 0 or pos.line > len(query_lines) or pos.column <= 0:
            return s + self.query
        query_lines = (
            query_lines[:pos.line]
            + [" " * (pos.column - 1) + "^"]
            + query_lines[pos.line:]
        )
        s += "\n".join(query_lines)
        return s
