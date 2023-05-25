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


import typing as t


if t.TYPE_CHECKING:
    import typing_extensions as te

import pytest

from neo4j import (
    Address,
    NotificationCategory,
    NotificationSeverity,
    ResultSummary,
    ServerInfo,
    SummaryCounters,
    SummaryNotification,
)


@pytest.fixture
def server_info_mock(mocker):
    server_info_mock = mocker.NonCallableMock()
    server_info_mock.protocol_version = (0, 0)
    return server_info_mock


@pytest.fixture
def address():
    return Address.parse("localhost:7687")


@pytest.fixture
def summary_args_kwargs(address, server_info_mock):
    return (address,), {"server": server_info_mock}


def test_summary_server(summary_args_kwargs) -> None:
    args, kwargs = summary_args_kwargs
    server_info_mock = kwargs["server"]

    summary = ResultSummary(*args, **kwargs)
    server_info: ServerInfo = summary.server

    assert server_info is server_info_mock
    assert not server_info_mock.method_calls


@pytest.mark.parametrize("exists", (True, False))
def test_summary_database(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["db"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[str] = summary.database

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_query(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["query"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[str] = summary.query

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_parameters(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["parameters"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[t.Dict[str, t.Any]] = summary.parameters

    assert summary_out is summary_in


@pytest.mark.parametrize("summary_in", (None, "w", "r", "rw", "s"))
def test_summary_query_type(summary_args_kwargs, summary_in) -> None:
    args, kwargs = summary_args_kwargs
    if summary_in is not None:
        kwargs["type"] = summary_in

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Union[None, te.Literal["r", "w", "rw", "s"]]
    summary_out = summary.query_type

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_plan(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["plan"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[dict] = summary.plan

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_profile(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["profile"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[dict] = summary.profile

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_notifications(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["notifications"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[t.List[dict]] = summary.notifications

    assert summary_out is summary_in


@pytest.mark.parametrize("summary_in", (
    [],
    [{"code": "foobar"}],
    [{"title": "foobar"}],
    [{"description": "foobar"}],
    *(
        [{"severity": s}]
        for s in ("WARNING", "INFORMATION", "UNKNOWN", "BANANA", "warning")
    ),
    *(
        [{"category": c}]
        for c in ("HINT", "QUERY", "UNRECOGNIZED", "UNSUPPORTED",
                  "PERFORMANCE", "DEPRECATION", "GENERIC", "UNKNOWN", "BANANA",
                  "hint")
    ),
    [
        {"code": "foobar"},
        {"title": "foobar"},
        {"description": "foobar"},
        {"severity": "WARNING"},
        {"category": "HINT"},
    ],
    [{"code": "foo"}, {"code": "bar"}],
    [{"title": "foo"}, {"title": "bar"}],
    [{"description": "foo"}, {"description": "bar"}],
    [{"severity": "WARNING"}, {"severity": "INFORMATION"}],
    [{"category": "HINT"}, {"category": "QUERY"}],
))
def test_summary_summary_notifications(
    summary_args_kwargs, summary_in
) -> None:
    args, kwargs = summary_args_kwargs
    if summary_in is not None:
        kwargs["notifications"] = summary_in

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.List[SummaryNotification] = summary.summary_notifications

    assert isinstance(summary_out, list)
    if summary_in is None:
        assert summary_out == []
        return

    assert summary_in is not None
    for notification_out, notification_in in zip(summary_out, summary_in):
        code_out: str = notification_out.code
        code_in = notification_in.get("code", "")
        assert code_out == code_in
        title_out: str = notification_out.title
        title_in = notification_in.get("title", "")
        assert title_out == title_in
        description_out: str = notification_out.description
        description_in = notification_in.get("description", "")
        assert description_out == description_in
        severity_out: NotificationSeverity = notification_out.severity_level
        try:
            severity_in = NotificationSeverity(notification_in.get("severity"))
        except ValueError:
            severity_in = NotificationSeverity.UNKNOWN
        assert severity_out == severity_in
        raw_severity_out: str = notification_out.raw_severity_level
        raw_severity_in = notification_in.get("severity", "")
        assert raw_severity_out == raw_severity_in
        category_out: NotificationCategory = notification_out.category
        try:
            category_in = NotificationCategory(notification_in.get("category"))
        except ValueError:
            category_in = NotificationCategory.UNKNOWN
        assert category_out == category_in
        raw_category_out: str = notification_out.raw_category
        raw_category_in = notification_in.get("category", "")
        assert raw_category_out == raw_category_in

        assert isinstance(notification_out, SummaryNotification)


UPDATE_FIELDS = {
    # server side key: summary attribute
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
}
SYSTEM_UPDATE_FIELDS = {
    # server side key: summary attribute
    "system-updates": "system_updates",
}


@pytest.mark.parametrize("counters_set", (
    None,
    {},
    {"foo": -1},
    *({k: v} for k in UPDATE_FIELDS for v in (0, 1, 42)),
    *({k: v} for k in SYSTEM_UPDATE_FIELDS for v in (0, 1, 42)),
    *(
        {k: v, "contains-updates": c}
        for k in UPDATE_FIELDS
        for v in (0, 1, 42)
        for c in (True, False)
    ),
    *(
        {k: v, "contains-system-updates": c}
        for k in SYSTEM_UPDATE_FIELDS
        for v in (0, 1, 42)
        for c in (True, False)
    ),
))
def test_summary_result_counters(summary_args_kwargs, counters_set) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = {}
    if counters_set is not None:
        kwargs["stats"] = summary_in = counters_set

    summary = ResultSummary(*args, **kwargs)
    summary_out: SummaryCounters = summary.counters

    for field, summary_attr in (*UPDATE_FIELDS.items(),
                               *SYSTEM_UPDATE_FIELDS.items()):
        assert getattr(summary_out, summary_attr) == summary_in.get(field, 0)

    contains_updates = any(getattr(summary_out, field) > 0
                           for field in UPDATE_FIELDS.values())
    assert (summary_out.contains_updates
            == summary_in.get("contains-updates", contains_updates))

    contains_system_updates = any(getattr(summary_out, field) > 0
                                  for field in SYSTEM_UPDATE_FIELDS.values())
    assert (summary_out.contains_system_updates
            == summary_in.get("contains-system-updates",
                              contains_system_updates))


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize("exists", (True, False))
@pytest.mark.parametrize(("bolt_version", "meta_name"), (
    ((2, 0), "result_available_after"),
    ((3, 0), "t_first"),
    ((4, 0), "t_first"),
    ((4, 1), "t_first"),
    ((4, 2), "t_first"),
    ((4, 3), "t_first"),
    ((4, 4), "t_first"),
    ((5, 0), "t_first"),
    ((5, 1), "t_first"),
    ((5, 2), "t_first"),
    ((5, 3), "t_first"),
))
def test_summary_result_available_after(
    summary_args_kwargs, exists, bolt_version, meta_name
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["server"].protocol_version = bolt_version
    summary_in = None
    if exists:
        kwargs[meta_name] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[int] = summary.result_available_after

    assert summary_out is summary_in


# [bolt-version-bump] search tag when changing bolt version support
@pytest.mark.parametrize("exists", (True, False))
@pytest.mark.parametrize(("bolt_version", "meta_name"), (
    ((2, 0), "result_consumed_after"),
    ((3, 0), "t_last"),
    ((4, 0), "t_last"),
    ((4, 1), "t_last"),
    ((4, 2), "t_last"),
    ((4, 3), "t_last"),
    ((4, 4), "t_last"),
    ((5, 0), "t_last"),
    ((5, 1), "t_last"),
    ((5, 2), "t_last"),
    ((5, 3), "t_last"),
))
def test_summary_result_consumed_after(
    summary_args_kwargs, exists, bolt_version, meta_name
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["server"].protocol_version = bolt_version
    summary_in = None
    if exists:
        kwargs[meta_name] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[int] = summary.result_consumed_after

    assert summary_out is summary_in
