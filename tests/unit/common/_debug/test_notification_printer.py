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

import pytest

from neo4j._debug import NotificationPrinter


if t.TYPE_CHECKING:
    from ...fixtures.notifications import (
        Position,
        TStatusNotificationFactory,
    )


@pytest.mark.parametrize(
    ("query", "position", "expected_output_template"),
    (
        # no query
        (None, None, "{gql_status}"),
        (None, {"offset": 0, "line": 1, "column": 1}, "{gql_status}"),
        # ---------------------------------------------------------------------
        # no position
        ("MATCH (n) RETURN n", None, "{gql_status} for query:\n{query}"),
        ("MATCH (n)\nRETURN n", None, "{gql_status} for query:\n{query}"),
        # ---------------------------------------------------------------------
        # normal position
        (
            "MATCH (n) RETURN n",
            {"offset": 0, "line": 1, "column": 1},
            (
                "{gql_status} for query:\n"
                "MATCH (n) RETURN n\n"
                "^"
            ),
        ),
        (
            "MATCH (n) RETURN n",
            {"offset": 2, "line": 1, "column": 3},
            (
                "{gql_status} for query:\n"
                "MATCH (n) RETURN n\n"
                "  ^"
            ),
        ),
        (
            (
                "MATCH (n)\n"
                "RETURN n"
            ),
            {"offset": 0, "line": 1, "column": 3},
            (
                "{gql_status} for query:\n"
                "MATCH (n)\n"
                "  ^\n"
                "RETURN n"
            ),
        ),
        (
            (
                "MATCH (n)\n"
                "RETURN n"
            ),
            {"offset": 0, "line": 2, "column": 8},
            (
                "{gql_status} for query:\n"
                "MATCH (n)\n"
                "RETURN n\n"
                "       ^"
            ),
        ),
        # ---------------------------------------------------------------------
        # position out of bounds
        *(
            (
                "MATCH (n) RETURN n",
                {"offset": 0, "line": line, "column": column},
                (
                    "{gql_status} for query:\n"
                    "MATCH (n) RETURN n"
                ),
            )
            for (line, column) in (
                (0, 1),
                (-1, 1),
                (2, 1),
                (3, 1),
                (1, 0),
                (1, -1),
            )
        ),
        (
            "MATCH (n) RETURN n",
            {"offset": 0, "line": 1, "column": 20},
            (
                "{gql_status} for query:\n"
                "MATCH (n) RETURN n\n"
                "                   ^"
            ),
        ),
        (
            (
                "MATCH (n)\n"
                "RETURN n"
            ),
            {"offset": 0, "line": 1, "column": 20},
            (
                "{gql_status} for query:\n"
                "MATCH (n)\n"
                "                   ^\n"
                "RETURN n"
            ),
        ),
    ),
)  # fmt: skip
def test_position(
    status_notification_factory: TStatusNotificationFactory,
    query: str | None,
    position: Position | None,
    expected_output_template: str,
) -> None:
    gql_status = status_notification_factory(
        diag_rec_overwrite={"_position": position}
    )
    printer = NotificationPrinter(gql_status, query)
    expected_output = expected_output_template.format(
        query=query, gql_status=repr(gql_status)
    )
    assert str(printer) == expected_output
