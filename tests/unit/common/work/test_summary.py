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
import warnings


if t.TYPE_CHECKING:
    import typing_extensions as te

    _T = t.TypeVar("_T")
    _TDict = t.TypeVar("_TDict", bound=dict)

import pytest

from neo4j import (
    Address,
    NotificationCategory,
    NotificationSeverity,
    PreviewWarning,
    ResultSummary,
    ServerInfo,
    SummaryCounters,
    SummaryInputPosition,
    SummaryNotification,
)


with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=PreviewWarning)
    from neo4j import (
        GqlStatusObject,
        NotificationClassification,
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
def summary_args_kwargs(address, server_info_mock) -> t.Tuple[tuple, dict]:
    return (
        (address,),
        {
            "had_key": True,
            "had_record": True,
            "metadata": {
                "server": server_info_mock,
            }
        }
    )


def test_summary_server(summary_args_kwargs) -> None:
    args, kwargs = summary_args_kwargs
    server_info_mock = kwargs["metadata"]["server"]

    summary = ResultSummary(*args, **kwargs)
    server_info: ServerInfo = summary.server

    assert server_info is server_info_mock
    assert not server_info_mock.method_calls


@pytest.mark.parametrize("exists", (True, False))
def test_summary_database(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["db"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[str] = summary.database

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_query(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["query"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[str] = summary.query

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_parameters(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["parameters"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[t.Dict[str, t.Any]] = summary.parameters

    assert summary_out is summary_in


@pytest.mark.parametrize("summary_in", (None, "w", "r", "rw", "s"))
def test_summary_query_type(summary_args_kwargs, summary_in) -> None:
    args, kwargs = summary_args_kwargs
    if summary_in is not None:
        kwargs["metadata"]["type"] = summary_in

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Union[None, te.Literal["r", "w", "rw", "s"]]
    summary_out = summary.query_type

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_plan(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["plan"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[dict] = summary.plan

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_profile(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["profile"] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[dict] = summary.profile

    assert summary_out is summary_in


@pytest.mark.parametrize("exists", (True, False))
def test_summary_notifications(summary_args_kwargs, exists) -> None:
    args, kwargs = summary_args_kwargs
    summary_in = None
    if exists:
        kwargs["metadata"]["notifications"] = summary_in = [object()]

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[t.List[dict]] = summary.notifications

    assert summary_out is summary_in


def test_statuses_and_notifications_dont_mix(summary_args_kwargs) -> None:
    raw_diag_rec = {
        "OPERATION": "",
        "OPERATION_CODE": "0",
        "CURRENT_SCHEMA": "/",
        "_status_parameters": {},
        "_severity": "WARNING",
        "_classification": "HINT",
        "_position": {
            "line": 1337,
            "column": 42,
            "offset": 420,
        },
    }
    raw_status = {
        "gql_status": "12345",
        "status_description": "cool description",
        "description": "cool notification description",
        "neo4j_code": "Neo.Foo.Bar.Baz",
        "title": "nice title",
        "diagnostic_record": raw_diag_rec,
    }

    raw_notification = {
        "code": "Neo.Foo.Bar.Biz",
        "description": "another description",
        "title": "completely different title",
        "severity": "INFORMATION",
        "category": "PERFORMANCE",
        "position": {
            "line": 42,
            "column": 1337,
            "offset": 0,
        },
    }

    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]
    kwargs["metadata"]["notifications"] = [raw_notification]

    summary = ResultSummary(*args, **kwargs)

    notifications = summary.notifications
    assert notifications == [raw_notification]

    parsed_notifications = summary.summary_notifications
    assert len(parsed_notifications) == 1
    notification = parsed_notifications[0]
    assert notification.code == raw_notification["code"]
    assert notification.description == raw_notification["description"]
    assert notification.title == raw_notification["title"]
    assert notification.severity_level == NotificationSeverity.INFORMATION
    assert notification.raw_severity_level == raw_notification["severity"]
    assert notification.category == NotificationCategory.PERFORMANCE
    assert notification.raw_category == raw_notification["category"]
    assert notification.position == SummaryInputPosition(
        line=42, column=1337, offset=0
    )

    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        statuses = summary.gql_status_objects
    assert len(statuses) == 1
    status = statuses[0]
    assert status.gql_status == raw_status["gql_status"]
    assert status.status_description == raw_status["status_description"]
    assert status.severity == NotificationSeverity.WARNING
    assert status.raw_severity == raw_diag_rec["_severity"]
    assert status.classification == NotificationClassification.HINT
    assert status.raw_classification == raw_diag_rec["_classification"]
    assert status.position == SummaryInputPosition(
        line=1337, column=42, offset=420
    )
    assert status.diagnostic_record == raw_diag_rec


def assert_is_non_notification_status(status: GqlStatusObject) -> None:
    is_notification: bool = status.is_notification
    assert not is_notification

    position: t.Optional[SummaryInputPosition] = status.position
    assert position is None

    raw_classification: t.Optional[str] = status.raw_classification
    assert raw_classification is None

    classification: t.Optional[NotificationClassification]
    classification = status.classification
    assert classification == NotificationClassification.UNKNOWN

    raw_severity: t.Optional[str] = status.raw_severity
    assert raw_severity is None

    severity: t.Optional[NotificationSeverity] = status.severity
    assert severity == NotificationSeverity.UNKNOWN

    diagnostic_record: t.Dict[str, t.Any] = status.diagnostic_record
    assert diagnostic_record == {
        "OPERATION": "",
        "OPERATION_CODE": "0",
        "CURRENT_SCHEMA": "/",
    }


STATUS_SUCCESS = {
    "gql_status": "00000",
    "status_description": "note: successful completion",
    "diagnostic_record": {
        "OPERATION": "",
        "OPERATION_CODE": "0",
        "CURRENT_SCHEMA": "/",
    }
}
STATUS_OMITTED_RESULT = {
    **STATUS_SUCCESS,
    "gql_status": "00001",
    "status_description": "note: successful completion - omitted result",
}
STATUS_NO_DATA = {
    **STATUS_SUCCESS,
    "gql_status": "02000",
    "status_description": "note: no data",
}


class StatusOrderHelper:
    @staticmethod
    def make_raw_status(
        i: int, type_: te.Literal["WARNING", "INFORMATION",
                                  "SUCCESS", "OMITTED", "NODATA"]
    ) -> dict:
        if type_ == "SUCCESS":
            return STATUS_SUCCESS
        if type_ == "OMITTED":
            return STATUS_OMITTED_RESULT
        if type_ == "NODATA":
            return STATUS_NO_DATA
        category = "01" if type_ == "WARNING" else "03"
        gql_status = f"{category}N{i:02d}"
        return {
            "gql_status": gql_status,
            "status_description": "note: successful completion - "
                                  f"custom stuff {i}",
            "description": f"notification description {i}",
            "neo4j_code": f"Neo.Foo.Bar.{type_}-{i}",
            "title": f"Some cool title which defo is dope! {i}",
            "diagnostic_record": {
                "OPERATION": "",
                "OPERATION_CODE": "0",
                "CURRENT_SCHEMA": "/",
                "_status_parameters": {},
                "_severity": type_,
                "_classification": "HINT",
                "_position": {
                    "line": 1337,
                    "column": 42,
                    "offset": 420
                },
            }
        }

    @staticmethod
    def assert_notification_is_status(
        notification: dict,
        i: int,
        type_: te.Literal["WARNING", "INFORMATION",
                          "SUCCESS", "OMITTED", "NODATA"]
    ) -> None:
        assert notification.get("code") == f"Neo.Foo.Bar.{type_}-{i}"

    @staticmethod
    def assert_parsed_notification_is_status(
        notification: SummaryNotification,
        i: int,
        type_: te.Literal["WARNING", "INFORMATION",
                          "SUCCESS", "OMITTED", "NODATA"]
    ) -> None:
        assert notification.code == f"Neo.Foo.Bar.{type_}-{i}"

    @staticmethod
    def assert_status_data_matches(
        status: GqlStatusObject,
        i: int,
        type_: te.Literal["WARNING", "INFORMATION",
                          "SUCCESS", "OMITTED", "NODATA"]
    ) -> None:
        if type_ == "SUCCESS":
            assert status.gql_status == STATUS_SUCCESS["gql_status"]
        elif type_ == "OMITTED":
            assert status.gql_status == STATUS_OMITTED_RESULT["gql_status"]
        elif type_ == "NODATA":
            assert status.gql_status == STATUS_NO_DATA["gql_status"]
        else:
            assert status.status_description.rsplit(" ", 1)[-1] == str(i)


@pytest.mark.parametrize(
    "raw_status",
    (STATUS_SUCCESS, STATUS_OMITTED_RESULT, STATUS_NO_DATA)
)
def test_non_notification_statuses(raw_status, summary_args_kwargs) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]

    summary = ResultSummary(*args, **kwargs)
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects: t.Sequence[GqlStatusObject] = \
            summary.gql_status_objects

    assert len(status_objects) == 1
    status = status_objects[0]

    gql_status: str = status.gql_status
    assert gql_status == raw_status["gql_status"]
    description: str = status.status_description
    assert description == raw_status["status_description"]
    assert_is_non_notification_status(status)


@pytest.mark.parametrize(
    "types",
    (
        ["SUCCESS"],
        ["OMITTED"],
        ["NODATA"],
        ["WARNING"],
        ["INFORMATION"],
        ["INFORMATION", "SUCCESS", "WARNING"],
        ["WARNING", "INFORMATION", "NODATA"],
        ["SUCCESS", "WARNING", "SUCCESS"],
        [
            "INFORMATION", "WARNING", "INFORMATION", "WARNING", "OMITTED",
            "OMITTED", "SUCCESS", "NODATA", "INFORMATION", "NODATA",
            "INFORMATION", "WARNING", "SUCCESS",
        ],
    )
)
def test_gql_statuses_keep_order(
    summary_args_kwargs,
    types,
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [
        StatusOrderHelper.make_raw_status(i, type_)
        for i, type_ in enumerate(types)
    ]
    summary = ResultSummary(*args, **kwargs)

    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects: t.Sequence[GqlStatusObject] = \
            summary.gql_status_objects

    assert len(status_objects) == len(types)
    status: GqlStatusObject
    for i, (type_, status) in enumerate(zip(types, status_objects)):
        StatusOrderHelper.assert_status_data_matches(
            status, i, type_
        )


@pytest.mark.parametrize(
    (
        "raw_status_overwrite",
        "expectation_overwrite",
    ),
    (
        ({}, {}),

        # gql_status
        #  * string values stay as is
        #  * invalid values get turned into ""
        ({"gql_status": ""}, {"gql_status": ""}),
        ({"gql_status": "00000"}, {"gql_status": "00000"}),
        (
            {"gql_status": "aBc1%d 👀\t Hi!!"},
            {"gql_status": "aBc1%d 👀\t Hi!!"}
        ),
        *(
            ({"gql_status": value}, {"gql_status": ""})
            for value in t.cast(t.Iterable, (1, None, False, ..., {}, []))
        ),

        # status_description is handled like gql_status
        ({"status_description": ""}, {"status_description": ""}),
        ({"status_description": "test"}, {"status_description": "test"}),
        (
            {"status_description": "aBc1%d 👀\t Hi!!"},
            {"status_description": "aBc1%d 👀\t Hi!!"}
        ),
        *(
            ({"status_description": value}, {"status_description": ""})
            for value in t.cast(t.Iterable, (1, None, False, ..., {}, []))
        ),

        # title doesn't matter
        ({"title": "some other title, doesn't matter"}, {}),
        ({"title": 1}, {}),
        ({"title": None}, {}),
        ({"title": ...}, {}),

        # neo4j_code doesn't matter except for determining is_notification
        ({"neo4j_code": "Neo.ClientError.You.Suck"}, {}),
        ({"neo4j_code": ""}, {"is_notification": False}),
        ({"neo4j_code": 1}, {"is_notification": False}),
        ({"neo4j_code": None}, {"is_notification": False}),
        ({"neo4j_code": ...}, {"is_notification": False}),

        # severity
        #  * know severities are mapped
        #  * stays as-is in the diagnostic record
        #  * invalid gets turned into `None`
        (
            {("diagnostic_record", "_severity"): "INFORMATION"},
            {"severity": "INFORMATION", "raw_severity": "INFORMATION"}
        ),
        (
            {("diagnostic_record", "_severity"): "WARNING"},
            {"severity": "WARNING", "raw_severity": "WARNING"}
        ),
        *(
            (
                {("diagnostic_record", "_severity"): sev},
                {"severity": "UNKNOWN", "raw_severity": sev}
            )
            for sev in ("", "FOOBAR", "UNKNOWN")
        ),
        *(
            (
                {("diagnostic_record", "_severity"): severity},
                {"severity": "UNKNOWN", "raw_severity": None}
            )
            for severity in (1, None, ...)
        ),

        # classification
        #  * know classifications are mapped to classification
        #  * stays as-is in the diagnostic record
        #  * invalid gets turned into `None`
        *(
            (
                {("diagnostic_record", "_classification"): cls},
                {"raw_classification": cls, "classification": cls}
            )
            for cls in NotificationClassification.__members__
            if cls != "UNKNOWN"
        ),
        *(
            (
                {("diagnostic_record", "_classification"): cls},
                {"raw_classification": cls, "classification": "UNKNOWN"}
            )
            for cls in ("", "FOOBAR", "UNKNOWN")
        ),
        *(
            (
                {("diagnostic_record", "_classification"): cls},
                {"classification": "UNKNOWN", "raw_classification": None}
            )
            for cls in (1, None, ...)
        ),

        # position
        #  * stays as-is in the diagnostic record
        #  * valid positions are mapped to status.position
        #  * invalid positions are mapped to None in status.position
        *(
            (
                {("diagnostic_record", "_position"): pos},
                {
                    "position": SummaryInputPosition(
                        line=pos["line"], column=pos["column"],
                        offset=pos["offset"]
                    )
                }
            )
            for pos in t.cast(
                t.Iterable[dict], (
                    {"line": 1, "column": 1, "offset": 1},
                    {"line": 999999, "column": 1, "offset": 1},
                    {"line": 1, "column": 999999, "offset": 1},
                    {"line": 1, "column": 1, "offset": 999999},
                    {"line": 999999, "column": 999999, "offset": 999999},
                    {"line": 0, "column": 1, "offset": 1},
                    {"line": 1, "column": 0, "offset": 1},
                    {"line": 1, "column": 1, "offset": 0},
                    {"line": 0, "column": 0, "offset": 0},
                    {"line": -1, "column": 1, "offset": 1},
                    {"line": 1, "column": -1, "offset": 1},
                    {"line": 1, "column": 1, "offset": -1},
                    {"line": -1, "column": -1, "offset": -1},
                    {"line": 1, "column": 1, "offset": 1, "extra": "hi"},
                    {"line": 1, "column": 1, "offset": 1, "extra": None},
                    {"line": 1, "column": 1, "offset": 1, "extra": 1},
                    {"line": 1, "column": 1, "offset": 1, "extra": 1.5},
                    {"line": 1, "column": 1, "offset": 1, "extra": []},
                )
            )
        ),
        *(
            (
                {("diagnostic_record", "_position"): pos},
                {"position": None}
            )
            for pos in (
                ...,
                "1",
                None,
                False,
                [1, 1, 1],
                {"column": 1, "offset": 1},
                {"line": 1, "offset": 1},
                {"line": 1, "column": 1},
                {"line": "1", "column": 1, "offset": 1},
                {"line": 1, "column": "1", "offset": 1},
                {"line": 1, "column": 1, "offset": "1"},
                {"line": True, "column": 1, "offset": 1},
                {"line": 1, "column": True, "offset": 1},
                {"line": 1, "column": 1, "offset": True},
                {"line": None, "column": 1, "offset": 1},
                {"line": 1, "column": None, "offset": 1},
                {"line": 1, "column": 1, "offset": None},
                {"line": 1.0, "column": 1, "offset": 1},
                {"line": 1, "column": 1.0, "offset": 1},
                {"line": 1, "column": 1, "offset": 1.0},
                {"line": [1], "column": 1, "offset": 1},
                {"line": 1, "column": [1], "offset": 1},
                {"line": 1, "column": 1, "offset": [1]},
            )
        ),
    )
)
def test_status(
    raw_status_overwrite, expectation_overwrite, summary_args_kwargs
) -> None:
    args, kwargs = summary_args_kwargs
    default_position = SummaryInputPosition(line=1337, column=42, offset=420)
    default_status_description = "some nice description goes here"
    default_description = "some nice notification description here"
    default_severity = "WARNING"
    default_classification = "HINT"
    default_code = "Neo.Cool.Legacy.Code"
    default_title = "Cool Title"
    default_gql_status = "12345"

    raw_status: t.Dict[str, t.Any] = {
        "gql_status": default_gql_status,
        "status_description": default_status_description,
        "description": default_description,
        "neo4j_code": default_code,
        "title": default_title,
        "diagnostic_record": {
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "CURRENT_SCHEMA": "/",
            "_status_parameters": {},
            "_severity": default_severity,
            "_classification": default_classification,
            "_position": {
                "line": default_position.line,
                "column": default_position.column,
                "offset": default_position.offset,
            },
        },
    }
    key: t.Union[str, t.Tuple[str, ...]]
    for key, value in raw_status_overwrite.items():
        raw_status_part = raw_status
        while isinstance(key, tuple):
            if len(key) == 0:
                raise ValueError("Cannot use empty key list")
            if len(key) == 1:
                key = key[0]
                break
            raw_status_part = raw_status_part[key[0]]
            key = key[1:]
        if value is ...:
            del raw_status_part[key]
        else:
            raw_status_part[key] = value
    kwargs["metadata"]["statuses"] = [raw_status]
    summary = ResultSummary(*args, **kwargs)

    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects: t.Sequence[GqlStatusObject] = \
            summary.gql_status_objects

    assert len(status_objects) == 1
    status = status_objects[0]
    assert (status.is_notification
            == expectation_overwrite.get("is_notification", True))
    assert (status.gql_status
            == expectation_overwrite.get("gql_status", default_gql_status))
    assert (status.status_description
            == expectation_overwrite.get("status_description",
                                         default_status_description))
    assert (status.position
            == expectation_overwrite.get("position", default_position))
    assert (status.raw_classification
            == expectation_overwrite.get("raw_classification",
                                         default_classification))
    if status.classification is not None:
        assert isinstance(status.classification, NotificationClassification)
    assert (status.classification
            == expectation_overwrite.get("classification",
                                         default_classification))
    assert (status.raw_severity
            == expectation_overwrite.get("raw_severity", default_severity))
    if status.severity is not None:
        assert isinstance(status.severity, NotificationSeverity)
    assert (status.severity
            == expectation_overwrite.get("severity", default_severity))
    assert status.diagnostic_record == raw_status["diagnostic_record"]


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
        kwargs["metadata"]["notifications"] = summary_in

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
        kwargs["metadata"]["stats"] = summary_in = counters_set

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
    ((5, 4), "t_first"),
    ((5, 5), "t_first"),
    ((5, 6), "t_first"),
))
def test_summary_result_available_after(
    summary_args_kwargs, exists, bolt_version, meta_name
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["server"].protocol_version = bolt_version
    summary_in = None
    if exists:
        kwargs["metadata"][meta_name] = summary_in = object()

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
    ((5, 4), "t_last"),
    ((5, 5), "t_last"),
    ((5, 6), "t_last"),
))
def test_summary_result_consumed_after(
    summary_args_kwargs, exists, bolt_version, meta_name
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["server"].protocol_version = bolt_version
    summary_in = None
    if exists:
        kwargs["metadata"][meta_name] = summary_in = object()

    summary = ResultSummary(*args, **kwargs)
    summary_out: t.Optional[int] = summary.result_consumed_after

    assert summary_out is summary_in


@pytest.mark.parametrize(
    ("had_key", "had_record", "expected_status", "expected_description"),
    (
        (True, True, "00000", "note: successful completion"),
        (True, False, "02000", "note: no data"),
        (
            False, False,
            "00001", "note: successful completion - omitted result"
        ),
        # (False, True, ...) is unspecified.
        # The server should never announce no keys and still return records.
    )
)
def test_status_from_no_notification(
    had_key: bool, had_record: bool,
    expected_status: str, expected_description: str,
    summary_args_kwargs
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["had_key"] = had_key
    kwargs["had_record"] = had_record
    summary = ResultSummary(*args, **kwargs)

    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects: t.Sequence[GqlStatusObject] = \
            summary.gql_status_objects

    assert len(status_objects) == 1
    status: GqlStatusObject = status_objects[0]

    assert_is_non_notification_status(status)

    gql_status: str = status.gql_status
    assert gql_status == expected_status

    status_description: str = status.status_description
    assert status_description == expected_description


INFORMATION_SEV_OVERWRITES = {
    "gql_status": "03N42",
    "severity": "INFORMATION",
    "raw_severity": "INFORMATION",
}
WARNING_SEV_OVERWRITES = {
    "gql_status": "01N42",
    "severity": "WARNING",
    "raw_severity": "WARNING",
}
FOOBAR_SEV_OVERWRITES = {
    "gql_status": "03N42",
    "severity": "UNKNOWN",
    "raw_severity": "FOOBAR",
}


@pytest.mark.parametrize(
    (
        "raw_notification_overwrite",
        "expectation_overwrite",
        "diag_record_expectation_overwrite",
    ),
    (
        ({}, {}, {}),

        # title doesn't matter
        ({"title": "some other title, doesn't matter"}, {}, {}),
        ({"title": 1}, {}, {}),
        ({"title": None}, {}, {}),
        ({"title": ...}, {}, {}),

        # code doesn't matter
        ({"code": "Neo.ClientError.You.Suck"}, {}, {}),
        ({"code": 1}, {}, {}),
        ({"code": None}, {}, {}),
        ({"code": ...}, {}, {}),

        # description is inferred from severity
        (
            {"description": ""},
            {"status_description": "warn: unknown warning"},
            {}
        ),
        (
            {"description": 1},
            {"status_description": "warn: unknown warning"},
            {}
        ),
        (
            {"description": None},
            {"status_description": "warn: unknown warning"},
            {}
        ),
        (
            {"description": ...},
            {"status_description": "warn: unknown warning"},
            {}
        ),
        (
            {"description": "", "severity": "INFORMATION"},
            {
                **INFORMATION_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "INFORMATION"}
        ),
        (
            {"description": 1, "severity": "INFORMATION"},
            {
                **INFORMATION_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "INFORMATION"}
        ),
        (
            {"description": None, "severity": "INFORMATION"},
            {
                **INFORMATION_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "INFORMATION"}
        ),
        (
            {"description": ..., "severity": "INFORMATION"},
            {
                **INFORMATION_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "INFORMATION"}
        ),
        (
            {"description": "", "severity": "FOOBAR"},
            {
                **FOOBAR_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "FOOBAR"}
        ),
        (
            {"description": 1, "severity": "FOOBAR"},
            {
                **FOOBAR_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "FOOBAR"}
        ),
        (
            {"description": None, "severity": "FOOBAR"},
            {
                **FOOBAR_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "FOOBAR"}
        ),
        (
            {"description": ..., "severity": "FOOBAR"},
            {
                **FOOBAR_SEV_OVERWRITES,
                "status_description":
                    "info: unknown notification",
            },
            {"_severity": "FOOBAR"}
        ),

        # severity
        #  * know severities are mapped
        #  * stays as-is in the diagnostic record
        #  * invalid gets turned into None
        (
            {"severity": "INFORMATION"},
            INFORMATION_SEV_OVERWRITES,
            {"_severity": "INFORMATION"}
        ),
        (
            {"severity": "WARNING"},
            WARNING_SEV_OVERWRITES,
            {"_severity": "WARNING"}
        ),
        *(
            (
                {"severity": severity},
                {
                    "gql_status": "03N42",
                    "severity": "UNKNOWN",
                    "raw_severity": severity,
                },
                {"_severity": severity}
            )
            for severity in ("FOOBAR", "")
        ),
        *(
            (
                {"severity": severity},
                {
                    "gql_status": "03N42",
                    "severity": "UNKNOWN",
                    "raw_severity": None,
                },
                {"_severity": severity}
            )
            for severity in (1, None, ...)
        ),

        # category
        #  * know categories are mapped to classification
        #  * stays as-is in the diagnostic record
        #  * invalid gets turned into "" (raw) / "UNKNOWN" (parsed enum)
        *(
            (
                {"category": cat},
                {"classification": cat, "raw_classification": cat},
                {"_classification": cat}
            )
            for cat in NotificationCategory.__members__
            if cat != "UNKNOWN"
        ),
        *(
            (
                {"category": cat},
                {"classification": "UNKNOWN", "raw_classification": cat},
                {"_classification": cat}
            )
            for cat in ("", "FOOBAR")
        ),
        *(
            (
                {"category": cat},
                {"classification": "UNKNOWN", "raw_classification": None},
                {"_classification": cat}
            )
            for cat in (1, None, ...)
        ),

        # position
        #  * stays as-is in the diagnostic record
        #  * valid positions are mapped to status.position
        #  * invalid positions are mapped to None in status.position
        *(
            (
                {"position": pos},
                {
                    "position": SummaryInputPosition(
                        line=pos["line"], column=pos["column"],
                        offset=pos["offset"]
                    )
                },
                {"_position": pos}
            )
            for pos in t.cast(
                t.Iterable[dict], (
                    {"line": 1, "column": 1, "offset": 1},
                    {"line": 999999, "column": 1, "offset": 1},
                    {"line": 1, "column": 999999, "offset": 1},
                    {"line": 1, "column": 1, "offset": 999999},
                    {"line": 999999, "column": 999999, "offset": 999999},
                    {"line": 0, "column": 1, "offset": 1},
                    {"line": 1, "column": 0, "offset": 1},
                    {"line": 1, "column": 1, "offset": 0},
                    {"line": 0, "column": 0, "offset": 0},
                    {"line": -1, "column": 1, "offset": 1},
                    {"line": 1, "column": -1, "offset": 1},
                    {"line": 1, "column": 1, "offset": -1},
                    {"line": -1, "column": -1, "offset": -1},
                    {"line": 1, "column": 1, "offset": 1, "extra": "hi"},
                    {"line": 1, "column": 1, "offset": 1, "extra": None},
                    {"line": 1, "column": 1, "offset": 1, "extra": 1},
                    {"line": 1, "column": 1, "offset": 1, "extra": 1.5},
                    {"line": 1, "column": 1, "offset": 1, "extra": []},
                )
            )
        ),
        *(
            (
                {"position": pos},
                {"position": None},
                {"_position": pos}
            )
            for pos in (
                ...,
                "1",
                None,
                False,
                [1, 1, 1],
                {"column": 1, "offset": 1},
                {"line": 1, "offset": 1},
                {"line": 1, "column": 1},
                {"line": "1", "column": 1, "offset": 1},
                {"line": 1, "column": "1", "offset": 1},
                {"line": 1, "column": 1, "offset": "1"},
                {"line": True, "column": 1, "offset": 1},
                {"line": 1, "column": True, "offset": 1},
                {"line": 1, "column": 1, "offset": True},
                {"line": None, "column": 1, "offset": 1},
                {"line": 1, "column": None, "offset": 1},
                {"line": 1, "column": 1, "offset": None},
                {"line": 1.0, "column": 1, "offset": 1},
                {"line": 1, "column": 1.0, "offset": 1},
                {"line": 1, "column": 1, "offset": 1.0},
                {"line": [1], "column": 1, "offset": 1},
                {"line": 1, "column": [1], "offset": 1},
                {"line": 1, "column": 1, "offset": [1]},
            )
        ),
    )
)
def test_status_from_notifications(
    raw_notification_overwrite, expectation_overwrite,
    diag_record_expectation_overwrite, summary_args_kwargs
) -> None:
    args, kwargs = summary_args_kwargs
    default_position = SummaryInputPosition(line=1337, column=42, offset=420)
    default_description = "some nice description goes here"
    default_severity = "WARNING"
    default_category = "HINT"
    raw_notification = {
        "title": "Cool Title",
        "code": "Neo.Cool.Legacy.Code",
        "description": default_description,
        "severity": default_severity,
        "category": default_category,
        "position": {
            "line": default_position.line,
            "column": default_position.column,
            "offset": default_position.offset,
        },
    }
    for key, value in raw_notification_overwrite.items():
        if value is ...:
            del raw_notification[key]
        else:
            raw_notification[key] = value
    kwargs["metadata"]["notifications"] = [raw_notification]
    summary = ResultSummary(*args, **kwargs)

    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects: t.Sequence[GqlStatusObject] = \
            summary.gql_status_objects
    notifications = [s for s in status_objects if s.is_notification]

    if "diagnostic_record" in expectation_overwrite:
        expected_diag_rec = expectation_overwrite["diagnostic_record"]
    else:
        expected_diag_rec = {
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "CURRENT_SCHEMA": "/",
            "_severity": default_severity,
            "_classification": default_category,
            "_position": {
                "line": default_position.line,
                "column": default_position.column,
                "offset": default_position.offset,
            }
        }
        for key, value in diag_record_expectation_overwrite.items():
            if value is ...:
                del expected_diag_rec[key]
            else:
                expected_diag_rec[key] = value

    assert len(notifications) == 1
    status = notifications[0]
    assert (status.gql_status
            == expectation_overwrite.get("gql_status", "01N42"))
    assert (status.status_description
            == expectation_overwrite.get("status_description",
                                         default_description))
    assert (status.position
            == expectation_overwrite.get("position", default_position))
    assert (status.raw_classification
            == expectation_overwrite.get("raw_classification",
                                         default_category))
    if status.classification is not None:
        assert isinstance(status.classification, NotificationClassification)
    assert (status.classification
            == expectation_overwrite.get("classification", default_category))
    assert (status.raw_severity
            == expectation_overwrite.get("raw_severity", default_severity))
    if status.severity is not None:
        assert isinstance(status.severity, NotificationSeverity)
    assert (status.severity
            == expectation_overwrite.get("severity", default_severity))
    assert status.diagnostic_record == expected_diag_rec

def update_summary_kwargs_result_type(
    kwargs: dict,
    result_type: te.Literal["success", "no data", "omitted result"]
) -> dict:
    if result_type == "success":
        kwargs["had_key"] = True
        kwargs["had_record"] = True
    elif result_type == "no data":
        kwargs["had_key"] = True
        kwargs["had_record"] = False
    elif result_type == "omitted result":
        kwargs["had_key"] = False
        kwargs["had_record"] = False
    else:
        raise ValueError(f"Unknown result_type: {result_type}")
    return kwargs


def make_notification_metadata(severity) -> t.Dict[str, t.Any]:
    return {
        "title": "Some Title",
        "code": "Neo.ClientWarning.Foo.Bar",
        "description": "Oops, I did it again :O",
        "severity": severity,
        "category": "HINT",
        "position": {
            "line": 3,
            "column": 5,
            "offset": 2,
        },
    }


@pytest.mark.parametrize(
    ("result_type", "severities", "expected_statuses"),
    (
        (
            "success",
            ["WARNING"],
            ["01N42", "00000"]
        ),
        (
            "no data",
            ["WARNING"],
            ["02000", "01N42"]
        ),
        (
            "omitted result",
            ["WARNING"],
            ["01N42", "00001"]
        ),
        (
            "success",
            ["INFORMATION"],
            ["00000", "03N42"]
        ),
        (
            "no data",
            ["INFORMATION"],
            ["02000", "03N42"]
        ),
        (
            "omitted result",
            ["INFORMATION"],
            ["00001", "03N42"]
        ),
        (
            "success",
            ["BANANA"],
            ["00000", "03N42"]
        ),
        (
            "no data",
            ["BANANA"],
            ["02000", "03N42"]
        ),
        (
            "omitted result",
            ["BANANA"],
            ["00001", "03N42"]
        ),
        (
            "success",
            ["WARNING", "INFORMATION", "FOO", "WARNING", "INFORMATION", "FOO"],
            ["01N42", "01N42", "00000", "03N42", "03N42", "03N42", "03N42"]
        ),
        (
            "no data",
            ["WARNING", "INFORMATION", "FOO", "WARNING", "INFORMATION", "FOO"],
            ["02000", "01N42", "01N42", "03N42", "03N42", "03N42", "03N42"]
        ),
        (
            "omitted result",
            ["WARNING", "INFORMATION", "FOO", "WARNING", "INFORMATION", "FOO"],
            ["01N42", "01N42", "00001", "03N42", "03N42", "03N42", "03N42"]
        ),
    )
)
def test_status_precedence(
    result_type, severities, expected_statuses,
    summary_args_kwargs
) -> None:
    args, kwargs = summary_args_kwargs
    update_summary_kwargs_result_type(kwargs, result_type)
    kwargs["metadata"]["notifications"] = list(
        make_notification_metadata(severity)
        for severity in severities
    )

    summary = ResultSummary(*args, **kwargs)
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        status_objects = summary.gql_status_objects
    gql_statuses = [s.gql_status for s in status_objects]

    assert gql_statuses == expected_statuses


@pytest.mark.parametrize(
    "raw_status",
    (STATUS_SUCCESS, STATUS_OMITTED_RESULT, STATUS_NO_DATA)
)
def test_no_notification_from_status(raw_status, summary_args_kwargs) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]

    summary = ResultSummary(*args, **kwargs)
    notifications: t.Optional[t.List[dict]] = summary.notifications
    summary_notifications: t.List[SummaryNotification] = \
        summary.summary_notifications

    assert notifications == []
    assert summary_notifications == []


@pytest.mark.parametrize(
    ("status_overwrite", "diagnostic_record_overwrite", "expected_overwrite"),
    (
        ({}, {}, {}),

        # have no effect on the produced notification
        #  * gql_status
        #  * diagnostic_record OPERATION
        #  * diagnostic_record OPERATION_CODE
        #  * diagnostic_record CURRENT_SCHEMA
        #  * diagnostic_record _status_parameters
        *(
            ({"gql_status": status}, {}, {})
            for status in t.cast(t.Iterable[t.Any],
                                 ("", None, ..., -1, 1.6, False, [], {}))
        ),
        *(
            ({}, {key: value}, {})
            for key in (
                "OPERATION",
                "OPERATION_CODE",
                "CURRENT_SCHEMA",
                "_status_parameters",
            )
            for value in t.cast(t.Iterable[t.Any],
                                ("FOOBAR", None, ..., -1, 1.6, False, [], {}))
        ),

        # copies description to description
        (
            {"description": "something completely different 👀"}, {},
            {"description": "something completely different 👀"}
        ),

        # copies title
        (
            {"title": "something completely different 👀"}, {},
            {"title": "something completely different 👀"}
        ),

        # _severity
        #  * unknown value gets turned into "" (raw) / "UNKNOWN" (parsed enum)
        #  * known value gets copied to raw and parsed
        (
            {}, {"_severity": "FOOBAR"},
            {"raw_severity": "FOOBAR", "severity": "UNKNOWN"}
        ),
        # (
        #     {}, {"_severity": ""},
        #     {"raw_severity": "", "severity": "UNKNOWN"}
        # ),
        *(
            (
                {}, {"_severity": sev},
                {"raw_severity": sev, "severity": sev}
            )
            for sev in NotificationSeverity.__members__
            if sev != "UNKNOWN"
        ),

        # _classification
        #  * maps to raw_category/category
        #  * unknown value gets turned into "" (raw) / "UNKNOWN" (parsed enum)
        #  * known value gets copied to raw and parsed
        (
            {}, {"_classification": "FOOBAR"},
            {"raw_category": "FOOBAR", "category": "UNKNOWN"}
        ),
        # (
        #     {}, {"_classification": ""},
        #     {"raw_category": "", "category": "UNKNOWN"}
        # ),
        *(
            (
                {}, {"_classification": sev},
                {"raw_category": sev, "category": sev}
            )
            for sev in NotificationCategory.__members__
            if sev != "UNKNOWN"
        ),

        # _position maps to position
        #  * extra fields are copied to raw data
        (
            {},
            {"_position": {"line": 1234, "column": 0, "offset": -9999}},
            {
                "raw_position": {"line": 1234, "column": 0, "offset": -9999},
                "position": SummaryInputPosition(
                    line=1234, column=0, offset=-9999
                ),
            }
        ),
        (
            {},
            {
                "_position": {
                    "line": 1234, "column": 0, "offset": -9999,
                    "extra": "hi :)"
                }
            },
            {
                "raw_position": {
                    "line": 1234, "column": 0, "offset": -9999,
                    "extra": "hi :)"
                },
                "position": SummaryInputPosition(
                    line=1234, column=0, offset=-9999
                ),
            }
        ),
    )
)
@pytest.mark.parametrize("filtered_statuses_front", (
    [],
    [STATUS_SUCCESS],
    [STATUS_OMITTED_RESULT],
    [STATUS_NO_DATA],
    [STATUS_SUCCESS, STATUS_OMITTED_RESULT, STATUS_NO_DATA],
))
@pytest.mark.parametrize("filtered_statuses_back", (
    [],
    [STATUS_SUCCESS],
    [STATUS_OMITTED_RESULT],
    [STATUS_NO_DATA],
    [STATUS_SUCCESS, STATUS_OMITTED_RESULT, STATUS_NO_DATA]
))
def test_notification_from_status(
    status_overwrite, diagnostic_record_overwrite, expected_overwrite,
    filtered_statuses_front, filtered_statuses_back,
    summary_args_kwargs
) -> None:
    default_status = "03BAZ"
    default_status_description = "note: successful completion - custom stuff"
    default_code = "Neo.Foo.Bar.Baz"
    default_title = "Some cool title which defo is dope!"
    default_severity = "INFORMATION"
    default_classification = "HINT"
    default_description = "nice message"
    default_position = SummaryInputPosition(line=1337, column=42, offset=420)
    raw_status_obj: t.Dict[str, t.Any] = {
        "gql_status": default_status,
        "status_description": default_status_description,
        "description": default_description,
        "neo4j_code": default_code,
        "title": default_title,
        "diagnostic_record": {
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "CURRENT_SCHEMA": "/",
            "_status_parameters": {},
            "_severity": default_severity,
            "_classification": default_classification,
            "_position": {
                "line": default_position.line,
                "column": default_position.column,
                "offset": default_position.offset,
            },
        }
    }
    for key, value in diagnostic_record_overwrite.items():
        if value is ...:
            del raw_status_obj["diagnostic_record"][key]
        else:
            raw_status_obj["diagnostic_record"][key] = value
    for key, value in status_overwrite.items():
        if value is ...:
            del raw_status_obj[key]
        else:
            raw_status_obj[key] = value

    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [
        *filtered_statuses_front, raw_status_obj, *filtered_statuses_back
    ]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    expected_notification = {
        "code": expected_overwrite.get("code", default_code),
        "title": expected_overwrite.get("title", default_title),
        "description":
            expected_overwrite.get("description", default_description),
        "severity": expected_overwrite.get("raw_severity", default_severity),
        "category":
            expected_overwrite.get("raw_category", default_classification),
        "position": expected_overwrite.get(
            "raw_position",
            {
                "line": default_position.line,
                "column": default_position.column,
                "offset": default_position.offset
            }
        ),
    }
    assert notifications == [expected_notification]
    assert len(summary_notifications) == 1
    summary_notification = summary_notifications[0]
    assert (summary_notification.title
            == expected_overwrite.get("title", default_title))
    assert (summary_notification.code
            == expected_overwrite.get("code", default_code))
    assert (summary_notification.description
            == expected_overwrite.get("description", default_description))
    assert isinstance(summary_notification.severity_level,
                      NotificationSeverity)
    assert (summary_notification.severity_level
            == expected_overwrite.get("severity", default_severity))
    assert isinstance(summary_notification.category, NotificationCategory)
    assert (summary_notification.category
            == expected_overwrite.get("category", default_classification))
    assert (summary_notification.raw_severity_level
            == expected_overwrite.get("raw_severity", default_severity))
    assert (summary_notification.raw_category
            == expected_overwrite.get("raw_category", default_classification))
    assert (summary_notification.position
            == expected_overwrite.get("position", default_position))


@pytest.mark.parametrize(
    "status_in",
    (
        None,
        1,
        False,
        ["neo4j_code"],
        "neo4j_code"
    )
)
def test_no_notification_from_wrong_type_status(
    status_in, summary_args_kwargs
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [status_in]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    assert notifications == []
    assert summary_notifications == []


def _get_from_dict(
    dict_: _TDict,
    key: t.Sequence,
    default: t.Any = None,
) -> t.Any:
    if not key:
        raise ValueError("key must not be empty")
    target = dict_
    for key_ in key[:-1]:
        if key_ not in target:
            return default
        target_any = target[key_]
        if not isinstance(target, dict):
            return default
        target = target_any
    return target.get(key[-1], default)


def _del_from_dict(
    dict_: _TDict,
    del_key: t.Sequence,
) -> _TDict:
    if not del_key:
        raise ValueError("del_key must not be empty")
    target = dict_
    for i, key in enumerate(del_key[:-1]):
        if not isinstance(target.get(key), dict):
            raise TypeError(
                f"Expected dict at {del_key!r}[{i}], got {target!r}"
            )
        target = target[key]
    del target[del_key[-1]]

    return dict_


def _set_in_dict(
    dict_: _TDict,
    set_key: t.Sequence,
    set_value: t.Any,
    *,
    create_if_missing: bool = True,
) -> _TDict:
    if not set_key:
        raise ValueError("set_key must not be empty")
    target = dict_
    for i, key in enumerate(set_key[:-1]):
        if create_if_missing:
            target = target.setdefault(key, {})
        else:
            if key not in target:
                return dict_
            target = target[key]
        if not isinstance(target, dict):
            raise TypeError(
                f"Expected dict at {set_key!r}[{i}], got {target!r}"
            )

    if not create_if_missing and set_key[-1] not in target:
        return dict_
    target[set_key[-1]] = set_value

    return dict_


def _make_raw_status_obj(
    *,
    del_keys: t.Iterable[t.Tuple[str, ...]] = (),
    replace: t.Optional[t.Dict[t.Tuple[str, ...], t.Any]] = None
) -> t.Dict[str, t.Any]:
    raw_status: t.Dict[str, t.Any] = {
        "gql_status": "03BAZ",
        "status_description": "note: successful completion - custom stuff",
        "description": "nice message",
        "neo4j_code": "Neo.Foo.Bar.Baz",
        "title": "Some cool title which defo is dope!",
        "diagnostic_record": {
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "CURRENT_SCHEMA": "/",
            "_status_parameters": {},
            "_severity": "INFORMATION",
            "_classification": "HINT",
            "_position": {
                "line": 1337,
                "column": 42,
                "offset": 420,
            },
        }
    }

    if replace is None:
        replace = {}
    for key, value in replace.items():
        _set_in_dict(raw_status, key, value)

    for del_key in del_keys:
        _del_from_dict(raw_status, del_key)

    return raw_status


def _make_raw_notification_obj(
    *,
    del_keys: t.Iterable[t.Tuple[str, ...]] = (),
    replace: t.Optional[t.Dict[t.Tuple[str, ...], t.Any]] = None
) -> t.Dict[str, t.Any]:
    raw_notification_obj: t.Dict[str, t.Any] = {
        "code": "Neo.Foo.Bar.Baz",
        "description": "nice message",
        "title": "Some cool title which defo is dope!",
        "severity": "INFORMATION",
        "category": "HINT",
        "position": {
            "line": 1337,
            "column": 42,
            "offset": 420,
        },
    }

    key_translation: t.Dict[
        t.Tuple[str, ...],
        t.Tuple[t.Tuple[str, ...], ...]
    ] = {
        ("neo4j_code",): (
            ("code",),
        ),
        ("description",): (
            ("description",),
        ),
        ("title",): (
            ("title",),
        ),
        ("diagnostic_record", "_severity"): (
            ("severity",),
        ),
        ("diagnostic_record",): (
            ("severity",),
            ("category",),
            ("position",),
        ),
        ("diagnostic_record", "_classification"): (
            ("category",),
        ),
        ("diagnostic_record", "_position"): (
            ("position",),
        ),
    }

    # dicts the driver will copy 1-to-1 including unknown keys
    copied_dicts = (
        ("diagnostic_record", "_position"),
    )

    del_keys_list = list(del_keys)

    if replace is None:
        replace = {}
    for key, value in replace.items():
        create_if_missing = False
        translated_keys = key_translation.get(key, ())
        if not translated_keys:
            for copied_dict in copied_dicts:
                if key[:len(copied_dict)] == copied_dict:
                    create_if_missing = True
                    translated_keys = tuple(
                        (*k, *key[len(copied_dict):])
                        for k in key_translation[copied_dict]
                    )
                    break
            else:
                continue
        if len(translated_keys) > 1:
            # original key points to dict/list
            # => cannot be translated by the driver's polyfill
            # => expect it to not be present in the polyfilled value
            del_keys_list.append(key)
            continue
        translated_key = translated_keys[0]
        _set_in_dict(
            raw_notification_obj,
            translated_key,
            value,
            # position is copied as is
            create_if_missing=create_if_missing
        )

    for key in del_keys_list:
        translated_keys = key_translation.get(key, ())
        if not translated_keys:
            for copied_dict in copied_dicts:
                if key[:len(copied_dict)] == copied_dict:
                    translated_keys = tuple(
                        (*k, *key[len(copied_dict):])
                        for k in key_translation[copied_dict]
                    )
                    break
                else:
                    continue
        for translated_key in translated_keys:
            _del_from_dict(raw_notification_obj, translated_key)

    return raw_notification_obj


def test_no_notification_from_status_without_neo4j_code(
    summary_args_kwargs
) -> None:
    raw_status = _make_raw_status_obj(del_keys=(("neo4j_code",),))

    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    assert notifications == []
    assert summary_notifications == []


@pytest.mark.parametrize(
    "del_key",
    (
        ("gql_status",),
        ("status_description",),
        ("description",),
        ("title",),
        ("diagnostic_record",),
        ("diagnostic_record", "OPERATION"),
        ("diagnostic_record", "OPERATION_CODE"),
        ("diagnostic_record", "CURRENT_SCHEMA"),
        ("diagnostic_record", "_status_parameters"),
        ("diagnostic_record", "_severity"),
        ("diagnostic_record", "_classification"),
        ("diagnostic_record", "_position"),
        ("diagnostic_record", "_position", "line"),
        ("diagnostic_record", "_position", "column"),
        ("diagnostic_record", "_position", "offset"),
    )
)
def test_notification_from_incomplete_status(
    del_key: t.Tuple[str, ...], summary_args_kwargs
) -> None:
    raw_status = _make_raw_status_obj(del_keys=(del_key,))
    raw_notification = _make_raw_notification_obj(del_keys=(del_key,))

    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    assert notifications == [raw_notification]

    assert summary_notifications == [
        SummaryNotification._from_metadata(raw_notification)
    ]


@pytest.mark.parametrize(
    "set_key",
    (
        ("neo4j_code",),
        ("gql_status",),
        ("status_description",),
        ("description",),
        ("title",),
        ("<shouldn't exist>",),
        ("diagnostic_record",),
        ("diagnostic_record", "OPERATION"),
        ("diagnostic_record", "OPERATION_CODE"),
        ("diagnostic_record", "CURRENT_SCHEMA"),
        ("diagnostic_record", "_status_parameters"),
        ("diagnostic_record", "_severity"),
        ("diagnostic_record", "_classification"),
        ("diagnostic_record", "<shouldn't exist>"),
        ("diagnostic_record", "_position"),
        ("diagnostic_record", "_position", "line"),
        ("diagnostic_record", "_position", "column"),
        ("diagnostic_record", "_position", "offset"),
        ("diagnostic_record", "_position", "<shouldn't exist>"),
    )
)
@pytest.mark.parametrize("set_value",
                         (None, 1, 3.14159, False, [], {}, "", "hi :)"))
def test_notification_from_unexpected_status(
    set_key: t.Tuple[str, ...], set_value: t.Any, summary_args_kwargs,
) -> None:
    replace = {set_key: set_value}
    raw_status = _make_raw_status_obj(replace=replace)
    raw_notification = _make_raw_notification_obj(replace=replace)

    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [raw_status]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    assert notifications == [raw_notification]

    assert summary_notifications == [
        SummaryNotification._from_metadata(raw_notification)
    ]


def _test_status():
    return {
        "gql_status": "12345",
        "status_description": "abcde",
        "neo4j_code": "Neo.Foo.Bar.Baz",
        "title": "title",
        "diagnostic_record": {
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "CURRENT_SCHEMA": "/",
            "_status_parameters": {},
            "_severity": "WARNING",
            "_classification": "HINT",
            "_position": {"line": 1, "column": 2, "offset": 3},
        }
    }


def _test_status_missing_key(key_path):
    status = _test_status()
    element = status
    for key in key_path[:-1]:
        element = element[key]
    del element[key_path[-1]]
    return status


def _test_status_broken_key(key_path, value):
    status = _test_status()
    element = status
    for key in key_path[:-1]:
        element = element[key]
    element[key_path[-1]] = value
    return status


@pytest.mark.parametrize(
    "in_status",
    (
        None,
        1,
        False,
        {},
        [],
        True,
    )
)
def test_notification_from_broken_status(
    in_status,
    summary_args_kwargs,
) -> None:
    args, kwargs = summary_args_kwargs
    kwargs["metadata"]["statuses"] = [in_status]

    summary = ResultSummary(*args, **kwargs)

    notifications = summary.notifications
    assert notifications == []


def test_notifications_from_statuses_keep_order(
    summary_args_kwargs,
) -> None:
    args, kwargs = summary_args_kwargs
    helper = StatusOrderHelper
    kwargs["metadata"]["statuses"] = [
        STATUS_NO_DATA,
        helper.make_raw_status(4, "WARNING"),
        helper.make_raw_status(2, "INFORMATION"),
        STATUS_SUCCESS,
        STATUS_NO_DATA,
        helper.make_raw_status(3, "INFORMATION"),
        helper.make_raw_status(1, "WARNING"),
        STATUS_OMITTED_RESULT,
    ]

    summary = ResultSummary(*args, **kwargs)
    notifications = summary.notifications
    summary_notifications = summary.summary_notifications

    assert notifications is not None
    assert len(notifications) == 4
    helper.assert_notification_is_status(notifications[0], 4, "WARNING")
    helper.assert_notification_is_status(notifications[1], 2, "INFORMATION")
    helper.assert_notification_is_status(notifications[2], 3, "INFORMATION")
    helper.assert_notification_is_status(notifications[3], 1, "WARNING")

    assert len(summary_notifications) == 4
    helper.assert_parsed_notification_is_status(summary_notifications[0],
                                                4, "WARNING")
    helper.assert_parsed_notification_is_status(summary_notifications[1],
                                                2, "INFORMATION")
    helper.assert_parsed_notification_is_status(summary_notifications[2],
                                                3, "INFORMATION")
    helper.assert_parsed_notification_is_status(summary_notifications[3],
                                                1, "WARNING")


def _make_summary_notification(
    overwrite: t.Optional[t.Dict[str, t.Any]] = None
) -> SummaryNotification:
    if overwrite is None:
        overwrite = {}
    return SummaryNotification(
        title=overwrite.get("title", ""),
        code=overwrite.get("code", ""),
        description=overwrite.get("description", ""),
        severity_level=overwrite.get(
            "severity_level", NotificationSeverity.UNKNOWN
        ),
        category=overwrite.get(
            "category", NotificationCategory.UNKNOWN
        ),
        raw_severity_level=overwrite.get("raw_severity_level", ""),
        raw_category=overwrite.get("raw_category", ""),
        position=overwrite.get("position")
    )


@pytest.mark.parametrize(
    ("meta", "expected"),
    (
        # empty meta data
        ({}, _make_summary_notification()),

        # all string fields
        *(
            (
                {key: str_value},
                _make_summary_notification({key: str_value})
            )
            for key in ("title", "code", "description")
            for str_value in ("", " ", "Foo Bar Baz")
        ),
        *(
            (
                {key: title},
                _make_summary_notification()
            )
            for key in ("title", "code", "description")
            for title in t.cast(t.Iterable,
                                (True, False, 1, [], {}, 1.2345, None))
        ),

        # severity
        *(
            (
                {"severity": raw_value},
                _make_summary_notification({
                    "raw_severity_level": raw_value,
                    "severity_level": parsed_value,
                })
            )
            for raw_value, parsed_value in (
                ("", NotificationSeverity.UNKNOWN),
                (" ", NotificationSeverity.UNKNOWN),
                ("Foo Bar", NotificationSeverity.UNKNOWN),
                ("WARNING", NotificationSeverity.WARNING),
                ("INFORMATION", NotificationSeverity.INFORMATION),
            )
        ),
        *(
            (
                {"severity": value},
                _make_summary_notification()
            )
            for value in t.cast(t.Iterable,
                                (True, False, 1, [], {}, 1.2345, None))
        ),

        # category
        *(
            (
                {"category": raw_value},
                _make_summary_notification({
                    "raw_category": raw_value,
                    "category": parsed_value,
                })
            )
            for raw_value, parsed_value in (
                ("", NotificationCategory.UNKNOWN),
                (" ", NotificationCategory.UNKNOWN),
                ("Foo Bar", NotificationCategory.UNKNOWN),
                ("HINT", NotificationCategory.HINT),
                ("UNRECOGNIZED", NotificationCategory.UNRECOGNIZED),
                ("UNSUPPORTED", NotificationCategory.UNSUPPORTED),
                ("PERFORMANCE", NotificationCategory.PERFORMANCE),
                ("DEPRECATION", NotificationCategory.DEPRECATION),
                ("GENERIC", NotificationCategory.GENERIC),
                ("SECURITY", NotificationCategory.SECURITY),
                ("TOPOLOGY", NotificationCategory.TOPOLOGY),
            )
        ),
        *(
            (
                {"category": value},
                _make_summary_notification()
            )
            for value in t.cast(t.Iterable,
                                (True, False, 1, [], {}, 1.2345, None))
        ),

        # position
        *(
            (
                {"position": raw_value},
                _make_summary_notification({"position": parsed_value})
            )
            for raw_value, parsed_value in (
                (
                    {"line": 1, "column": 2, "offset": 3},
                    SummaryInputPosition(line=1, column=2, offset=3)
                ),
                (
                    {"line": -100, "column": -200, "offset": -300},
                    SummaryInputPosition(line=-100, column=-200, offset=-300)
                ),
                (
                    {"line": 0, "column": 0, "offset": 0},
                    SummaryInputPosition(line=0, column=0, offset=0)
                ),
                # extraneous values are ignored
                *(
                    (
                        {"line": 1, "column": 2, "offset": 3, "extra": extra},
                        SummaryInputPosition(line=1, column=2, offset=3)
                    )
                    for extra in t.cast(
                        t.Iterable,
                        (None, 1, False, [], {}, "hi :)", "5", 1.2345)
                    )
                ),
            )
        ),
        # missing field/field of wrong type => position is default value
        *(
            (
                {"position": value},
                _make_summary_notification()
            )
            for value in (
                *(
                    {"line": 1, "column": 2, "offset": 3, key: value}
                    for key in ("line", "column", "offset")
                    for value in t.cast(t.Iterable,
                                        (1.2, None, False, True, [], {}, "1"))
                ),
                {"column": 2, "offset": 3},
                {"line": 1, "offset": 3},
                {"line": 1, "column": 2},
            )
        ),
    )
)
def test_notification_from_meta_data(
    meta: t.Dict,
    expected: SummaryNotification,
) -> None:
    notification = SummaryNotification._from_metadata(meta)
    assert notification == expected
