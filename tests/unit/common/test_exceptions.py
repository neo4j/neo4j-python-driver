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

import contextlib
import re
import traceback

import pytest

import neo4j.exceptions
from neo4j import PreviewWarning
from neo4j._exceptions import (
    BoltError,
    BoltHandshakeError,
    BoltProtocolError,
)
from neo4j._sync.io import Bolt
from neo4j.exceptions import (
    CLASSIFICATION_CLIENT,
    CLASSIFICATION_DATABASE,
    CLASSIFICATION_TRANSIENT,
    ClientError,
    DatabaseError,
    GqlError,
    Neo4jError,
    ServiceUnavailable,
    TransientError,
)


def test_bolt_error():
    with pytest.raises(BoltError) as e:
        error = BoltError("Error Message", address="localhost")
        assert repr(error) == "BoltError('Error Message')"
        assert str(error) == "Error Message"
        assert error.args == ("Error Message",)
        assert error.address == "localhost"
        raise error

    # The regexp parameter of the match method is matched with the re.search
    # function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    assert e.match("Error Message")


def test_bolt_protocol_error():
    with pytest.raises(BoltProtocolError) as e:
        error = BoltProtocolError(
            f"Driver does not support Bolt protocol version: 0x{2:06X}{5:02X}",
            address="localhost",
        )
        assert error.address == "localhost"
        raise error

    # The regexp parameter of the match method is matched with the re.search
    # function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")


def test_bolt_handshake_error():
    handshake = (
        b"\x00\x00\x00\x04\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00"
    )
    response = b"\x00\x00\x00\x00"
    supported_versions = Bolt.protocol_handlers().keys()

    with pytest.raises(BoltHandshakeError) as e:
        error = BoltHandshakeError(
            "The Neo4J server does not support communication with this "
            f"driver. Supported Bolt Protocols {supported_versions}",
            address="localhost",
            request_data=handshake,
            response_data=response,
        )
        assert error.address == "localhost"
        assert error.request_data == handshake
        assert error.response_data == response
        raise error

    e.match(
        "The Neo4J server does not support communication with this driver. "
        "Supported Bolt Protocols "
    )


def test_serviceunavailable():
    with pytest.raises(ServiceUnavailable) as e:
        error = ServiceUnavailable("Test error message")
        raise error

    assert e.value.__cause__ is None


def test_serviceunavailable_raised_from_bolt_protocol_error_with_implicit_style():  # noqa: E501
    error = BoltProtocolError(
        f"Driver does not support Bolt protocol version: 0x{2:06X}{5:02X}",
        address="localhost",
    )
    with pytest.raises(ServiceUnavailable) as e:
        assert error.address == "localhost"
        try:
            raise error
        except BoltProtocolError as error_bolt_protocol:
            raise ServiceUnavailable(
                str(error_bolt_protocol)
            ) from error_bolt_protocol

    # The regexp parameter of the match method is matched with the re.search
    # function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")
    assert e.value.__cause__ is error


def test_serviceunavailable_raised_from_bolt_protocol_error_with_explicit_style():  # noqa: E501
    error = BoltProtocolError(
        f"Driver does not support Bolt protocol version: 0x{2:06X}{5:02X}",
        address="localhost",
    )

    with pytest.raises(ServiceUnavailable) as e:
        assert error.address == "localhost"
        try:
            raise error
        except BoltProtocolError as error_bolt_protocol:
            error_nested = ServiceUnavailable(str(error_bolt_protocol))
            raise error_nested from error_bolt_protocol

    # The regexp parameter of the match method is matched with the re.search
    # function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")
    assert e.value.__cause__ is error


def _assert_default_gql_error_attrs_from_neo4j_error(error: GqlError) -> None:
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        assert error.gql_status == "50N42"
    if error.message:
        with pytest.warns(PreviewWarning, match="GQLSTATUS"):
            assert error.gql_status_description == (
                "error: general processing exception - unexpected error. "
                f"{error.message}"
            )
    else:
        with pytest.warns(PreviewWarning, match="GQLSTATUS"):
            assert error.gql_status_description == (
                "error: general processing exception - unexpected error"
            )
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        assert (
            error.gql_classification
            == neo4j.exceptions.GqlErrorClassification.UNKNOWN
        )
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        assert error.gql_raw_classification is None
    with pytest.warns(PreviewWarning, match="GQLSTATUS"):
        assert error.diagnostic_record == {
            "CURRENT_SCHEMA": "/",
            "OPERATION": "",
            "OPERATION_CODE": "0",
        }
    assert error.__cause__ is None


def test_neo4jerror_hydrate_with_no_args():
    error = Neo4jError._hydrate_neo4j()

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "An unknown error occurred"
    assert error.code == "Neo.DatabaseError.General.UnknownError"
    _assert_default_gql_error_attrs_from_neo4j_error(error)


def test_neo4jerror_hydrate_with_message_and_code_rubbish():
    error = Neo4jError._hydrate_neo4j(
        message="Test error message", code="ASDF_asdf"
    )

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "ASDF_asdf"
    _assert_default_gql_error_attrs_from_neo4j_error(error)


def test_neo4jerror_hydrate_with_message_and_code_database():
    error = Neo4jError._hydrate_neo4j(
        message="Test error message",
        code="Neo.DatabaseError.General.UnknownError",
    )

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "Neo.DatabaseError.General.UnknownError"
    _assert_default_gql_error_attrs_from_neo4j_error(error)


def test_neo4jerror_hydrate_with_message_and_code_transient():
    error = Neo4jError._hydrate_neo4j(
        message="Test error message",
        code="Neo.TransientError.General.TestError",
    )

    assert isinstance(error, TransientError)
    assert error.classification == CLASSIFICATION_TRANSIENT
    assert error.category == "General"
    assert error.title == "TestError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == f"Neo.{CLASSIFICATION_TRANSIENT}.General.TestError"
    _assert_default_gql_error_attrs_from_neo4j_error(error)


def test_neo4jerror_hydrate_with_message_and_code_client():
    error = Neo4jError._hydrate_neo4j(
        message="Test error message",
        code=f"Neo.{CLASSIFICATION_CLIENT}.General.TestError",
    )

    assert isinstance(error, ClientError)
    assert error.classification == CLASSIFICATION_CLIENT
    assert error.category == "General"
    assert error.title == "TestError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == f"Neo.{CLASSIFICATION_CLIENT}.General.TestError"
    _assert_default_gql_error_attrs_from_neo4j_error(error)


@pytest.mark.parametrize(
    ("code", "expected_cls", "expected_code"),
    (
        (
            "Neo.TransientError.Transaction.Terminated",
            ClientError,
            "Neo.ClientError.Transaction.Terminated",
        ),
        (
            "Neo.ClientError.Transaction.Terminated",
            ClientError,
            "Neo.ClientError.Transaction.Terminated",
        ),
        (
            "Neo.TransientError.Transaction.LockClientStopped",
            ClientError,
            "Neo.ClientError.Transaction.LockClientStopped",
        ),
        (
            "Neo.ClientError.Transaction.LockClientStopped",
            ClientError,
            "Neo.ClientError.Transaction.LockClientStopped",
        ),
        (
            "Neo.ClientError.Security.AuthorizationExpired",
            TransientError,
            "Neo.ClientError.Security.AuthorizationExpired",
        ),
        (
            "Neo.TransientError.General.TestError",
            TransientError,
            "Neo.TransientError.General.TestError",
        ),
    ),
)
@pytest.mark.parametrize("mode", ("neo4j", "gql"))
def test_error_rewrite(code, expected_cls, expected_code, mode):
    message = "Test error message"
    if mode == "neo4j":
        error = Neo4jError._hydrate_neo4j(message=message, code=code)
    elif mode == "gql":
        error = Neo4jError._hydrate_gql(
            gql_status="12345",
            description="error: things - they hit the fan",
            message=message,
            neo4j_code=code,
        )
    else:
        raise ValueError(f"Invalid mode {mode!r}")

    expected_retryable = expected_cls is TransientError
    assert error.__class__ is expected_cls
    assert error.code == expected_code
    assert error.message == message
    assert error.is_retryable() is expected_retryable
    with pytest.warns(DeprecationWarning, match=".*is_retryable.*"):
        assert error.is_retriable() is expected_retryable


@pytest.mark.parametrize(
    ("code", "message", "expected_cls", "expected_str", "mode"),
    (
        # values that behave the same in both modes
        *(
            (
                *x,
                mode,
            )
            for mode in ("neo4j", "gql")
            for x in (
                (
                    "Neo.ClientError.General.UnknownError",
                    "Test error message",
                    ClientError,
                    (
                        "{code: Neo.ClientError.General.UnknownError} "
                        "{message: Test error message}"
                    ),
                ),
                (
                    None,
                    "Test error message",
                    DatabaseError,
                    (
                        "{code: Neo.DatabaseError.General.UnknownError} "
                        "{message: Test error message}"
                    ),
                ),
                (
                    "Neo.ClientError.General.UnknownError",
                    None,
                    ClientError,
                    (
                        "{code: Neo.ClientError.General.UnknownError} "
                        "{message: An unknown error occurred}"
                    ),
                ),
            )
        ),
        # neo4j error specific behavior
        (
            "",
            "Test error message",
            DatabaseError,
            (
                "{code: Neo.DatabaseError.General.UnknownError} "
                "{message: Test error message}"
            ),
            "neo4j",
        ),
        (
            "Neo.ClientError.General.UnknownError",
            "",
            ClientError,
            "{code: Neo.ClientError.General.UnknownError} "
            "{message: An unknown error occurred}",
            "neo4j",
        ),
        # gql error specific behavior
        (
            "",
            "Test error message",
            DatabaseError,
            "{code: } {message: Test error message}",
            "gql",
        ),
        (
            "Neo.ClientError.General.UnknownError",
            "",
            ClientError,
            "{code: Neo.ClientError.General.UnknownError} {message: }",
            "gql",
        ),
    ),
)
def test_neo4j_error_from_server_as_str(
    code, message, expected_cls, expected_str, mode
):
    if mode == "neo4j":
        error = Neo4jError._hydrate_neo4j(code=code, message=message)
    elif mode == "gql":
        error = Neo4jError._hydrate_gql(
            gql_status="12345",
            description="error: things - they hit the fan",
            neo4j_code=code,
            message=message,
        )
    else:
        raise ValueError(f"Invalid mode {mode!r}")

    assert type(error) is expected_cls
    assert str(error) == expected_str


@pytest.mark.parametrize("cls", (Neo4jError, ClientError))
def test_neo4j_error_from_code_as_str(cls):
    error = cls("Generated somewhere in the driver")

    assert type(error) is cls
    assert str(error) == "Generated somewhere in the driver"


def _make_test_gql_error(
    identifier: str,
    cause: GqlError | None = None,
) -> GqlError:
    error = GqlError(identifier)
    error._init_gql(
        gql_status=f"{identifier[:5].upper():<05}",
        description=f"error: $h!t went down - {identifier}",
        message=identifier,
        cause=cause,
    )
    return error


def _set_error_cause(exc, cause, method="set") -> None:
    if method == "set":
        exc.__cause__ = cause
    elif method == "raise":
        with contextlib.suppress(exc.__class__):
            raise exc from cause
    else:
        raise ValueError(f"Invalid cause set method {method!r}")


_CYCLIC_CAUSE_MARKER = object()


def _assert_error_chain(
    exc: BaseException,
    expected: list[object],
) -> None:
    assert isinstance(exc, BaseException)

    collection_root: BaseException | None = exc
    actual_chain: list[object] = [exc]
    actual_chain_ids = [id(exc)]
    while collection_root is not None:
        cause = getattr(collection_root, "__cause__", None)
        if id(cause) in actual_chain_ids:
            actual_chain.append(_CYCLIC_CAUSE_MARKER)
            actual_chain_ids.append(id(_CYCLIC_CAUSE_MARKER))
            break
        actual_chain.append(cause)
        actual_chain_ids.append(id(cause))
        collection_root = cause

    assert actual_chain_ids == list(map(id, expected))

    expected_lines = [
        str(exc)
        for exc in expected
        if exc is not None and exc is not _CYCLIC_CAUSE_MARKER
    ]
    expected_lines.reverse()
    exc_fmt = traceback.format_exception(type(exc), exc, exc.__traceback__)
    for line in exc_fmt:
        if not expected_lines:
            break
        if expected_lines[0] in line:
            expected_lines.pop(0)
    if expected_lines:
        traceback_fmt = "".join(exc_fmt)
        pytest.fail(
            f"Expected lines not found: {expected_lines} in traceback:\n"
            f"{traceback_fmt}"
        )


def test_cause_chain_extension_no_cause() -> None:
    root = _make_test_gql_error("root")

    _assert_error_chain(root, [root, None])


def test_cause_chain_extension_only_gql_cause() -> None:
    root_cause = _make_test_gql_error("rootCause")
    root = _make_test_gql_error("root", cause=root_cause)

    _assert_error_chain(root, [root, root_cause, None])


@pytest.mark.parametrize("local_cause_method", ("raise", "set"))
def test_cause_chain_extension_only_local_cause(local_cause_method) -> None:
    root_cause = ClientError("rootCause")
    root = _make_test_gql_error("root")
    _set_error_cause(root, root_cause, local_cause_method)

    _assert_error_chain(root, [root, root_cause, None])


@pytest.mark.parametrize("local_cause_method", ("raise", "set"))
def test_cause_chain_extension_multiple_causes(local_cause_method) -> None:
    root4_cause2 = _make_test_gql_error("r4c2")
    root4_cause1 = _make_test_gql_error("r4c1", cause=root4_cause2)
    root4 = _make_test_gql_error("root4", cause=root4_cause1)
    root3 = ClientError("root3")
    _set_error_cause(root3, root4, local_cause_method)
    root2_cause3 = _make_test_gql_error("r2c3")
    root2_cause2 = _make_test_gql_error("r2c2", cause=root2_cause3)
    root2_cause1 = _make_test_gql_error("r2c1", cause=root2_cause2)
    root2 = _make_test_gql_error("root2", cause=root2_cause1)
    _set_error_cause(root2, root3, local_cause_method)
    root1_cause2 = _make_test_gql_error("r1c2")
    root1_cause1 = _make_test_gql_error("r1c1", cause=root1_cause2)
    root1 = _make_test_gql_error("root1", cause=root1_cause1)
    _set_error_cause(root1, root2, local_cause_method)

    _assert_error_chain(
        root1, [
            root1, root1_cause1, root1_cause2,
            root2, root2_cause1, root2_cause2, root2_cause3,
            root3,
            root4, root4_cause1, root4_cause2,
            None,
        ],
    )  # fmt: skip


@pytest.mark.parametrize("local_cause_method", ("raise", "set"))
def test_cause_chain_extension_circular_local_causes(
    local_cause_method,
) -> None:
    root6 = ClientError("root6")
    root5 = _make_test_gql_error("root5")
    _set_error_cause(root5, root6, local_cause_method)
    root4_cause = _make_test_gql_error("r4c")
    root4 = _make_test_gql_error("root4", cause=root4_cause)
    _set_error_cause(root4, root5, local_cause_method)
    root3 = ClientError("root3")
    _set_error_cause(root3, root4, local_cause_method)
    root2 = _make_test_gql_error("root2")
    _set_error_cause(root2, root3, local_cause_method)
    root1 = ClientError("root1")
    _set_error_cause(root1, root2, local_cause_method)
    _set_error_cause(root6, root1, local_cause_method)

    _assert_error_chain(
        root1,
        [
            root1,
            root2,
            root3,
            root4,
            root4_cause,
            root5,
            root6,
            _CYCLIC_CAUSE_MARKER,
        ],
    )


_DEFAULT_GQL_ERROR_ATTRIBUTES = {
    "code": "Neo.DatabaseError.General.UnknownError",
    "classification": "DatabaseError",
    "category": "General",
    "title": "UnknownError",
    "message": "An unknown error occurred",
    "gql_status": "50N42",
    "gql_status_description": (
        "error: general processing exception - unexpected error"
    ),
    "gql_classification": neo4j.exceptions.GqlErrorClassification.UNKNOWN,
    "gql_raw_classification": None,
    "diagnostic_record": {
        "CURRENT_SCHEMA": "/",
        "OPERATION": "",
        "OPERATION_CODE": "0",
    },
    "__cause__": None,
}


@pytest.mark.parametrize(
    ("metadata", "attributes"),
    (
        # all default values
        (
            {},
            _DEFAULT_GQL_ERROR_ATTRIBUTES,
        ),
        # example from ADR
        (
            {
                "gql_status": "01N00",
                "message": "01EXAMPLE you have failed something",
                "description": "client error - example error. Message",
                "neo4j_code": "Neo.Example.Failure.Code",
                "diagnostic_record": {
                    "CURRENT_SCHEMA": "",
                    "OPERATION": "",
                    "OPERATION_CODE": "",
                    "_classification": "CLIENT_ERROR",
                    "_status_parameters": {},
                },
            },
            {
                "code": "Neo.Example.Failure.Code",
                "classification": "Example",
                "category": "Failure",
                "title": "Code",
                "message": "01EXAMPLE you have failed something",
                "gql_status": "01N00",
                "gql_status_description": (
                    "client error - example error. Message"
                ),
                "gql_classification": (
                    neo4j.exceptions.GqlErrorClassification.CLIENT_ERROR
                ),
                "gql_raw_classification": "CLIENT_ERROR",
                "diagnostic_record": {
                    "CURRENT_SCHEMA": "",
                    "OPERATION": "",
                    "OPERATION_CODE": "",
                    "_classification": "CLIENT_ERROR",
                    "_status_parameters": {},
                },
                "__cause__": None,
            },
        ),
        # garbage diagnostic record
        (
            {
                "diagnostic_record": {
                    "CURRENT_SCHEMA": 1.5,
                    "OPERATION": False,
                    "_classification": ["whelp", None],
                    "_🤡": "🎈",
                    "foo": {"bar": "baz"},
                },
            },
            {
                **_DEFAULT_GQL_ERROR_ATTRIBUTES,
                "gql_classification": (
                    neo4j.exceptions.GqlErrorClassification.UNKNOWN
                ),
                "gql_raw_classification": None,
                "diagnostic_record": {
                    "CURRENT_SCHEMA": 1.5,
                    "OPERATION": False,
                    "_classification": ["whelp", None],
                    "_🤡": "🎈",
                    "foo": {"bar": "baz"},
                },
            },
        ),
        (
            {
                "diagnostic_record": {
                    "_classification": "SOME_FUTURE_CLASSIFICATION",
                },
            },
            {
                **_DEFAULT_GQL_ERROR_ATTRIBUTES,
                "gql_classification": (
                    neo4j.exceptions.GqlErrorClassification.UNKNOWN
                ),
                "gql_raw_classification": "SOME_FUTURE_CLASSIFICATION",
                "diagnostic_record": {
                    "_classification": "SOME_FUTURE_CLASSIFICATION",
                },
            },
        ),
    ),
)
def test_gql_hydration(metadata, attributes):
    # TODO: test causes
    error = Neo4jError._hydrate_gql(**metadata)

    preview_attrs = {
        "gql_status",
        "gql_status_description",
        "gql_classification",
        "gql_raw_classification",
        "diagnostic_record",
    }

    for attr in (
        "code",
        "classification",
        "category",
        "title",
        "message",
        "gql_status",
        "gql_status_description",
        "gql_classification",
        "gql_raw_classification",
        "diagnostic_record",
        "__cause__",
    ):
        expected_value = attributes[attr]
        if attr in preview_attrs:
            with pytest.warns(PreviewWarning, match="GQLSTATUS"):
                actual_value = getattr(error, attr)
        else:
            actual_value = getattr(error, attr)
        assert actual_value == expected_value


@pytest.mark.parametrize(
    "attr",
    (
        "code",
        "classification",
        "category",
        "title",
        "message",
        "metadata",
    ),
)
def test_deprecated_setter(attr):
    obj = object()
    error = Neo4jError()

    with pytest.warns(
        DeprecationWarning,
        match=re.compile(
            rf".*\baltering\b.*\b{attr}\b.*",
            flags=re.IGNORECASE,
        ),
    ):
        setattr(error, attr, obj)

    assert getattr(error, attr) is obj
