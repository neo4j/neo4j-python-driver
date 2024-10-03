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

# ruff: noqa: N818
# Not going to rename all Error classes that don't end on Error,
# which would break pretty much all users just to please the linter.


"""
Module containing the core driver exceptions.

Driver API Errors
=================
+ Neo4jError
  + ClientError
    + CypherSyntaxError
    + CypherTypeError
    + ConstraintError
    + AuthError
      + TokenExpired
    + Forbidden
  + DatabaseError
  + TransientError
    + DatabaseUnavailable
    + NotALeader
    + ForbiddenOnReadOnlyDatabase

+ DriverError
  + SessionError
  + TransactionError
    + TransactionNestingError
  + ResultError
    + ResultFailedError
    + ResultConsumedError
    + ResultNotSingleError
  + BrokenRecordError
  + SessionExpired
  + ServiceUnavailable
    + RoutingServiceUnavailable
    + WriteServiceUnavailable
    + ReadServiceUnavailable
    + IncompleteCommit
  + ConfigurationError
    + AuthConfigurationError
    + CertificateConfigurationError
"""

from __future__ import annotations

import typing as t
from copy import deepcopy as _deepcopy
from enum import Enum as _Enum

from ._meta import (
    deprecated,
    preview as _preview,
)


__all__ = [
    "AuthConfigurationError",
    "AuthError",
    "BrokenRecordError",
    "CertificateConfigurationError",
    "ClientError",
    "ConfigurationError",
    "ConstraintError",
    "CypherSyntaxError",
    "CypherTypeError",
    "DatabaseError",
    "DatabaseUnavailable",
    "DriverError",
    "Forbidden",
    "ForbiddenOnReadOnlyDatabase",
    "GqlError",
    "GqlErrorClassification",
    "IncompleteCommit",
    "Neo4jError",
    "NotALeader",
    "ReadServiceUnavailable",
    "ResultConsumedError",
    "ResultError",
    "ResultFailedError",
    "ResultNotSingleError",
    "RoutingServiceUnavailable",
    "ServiceUnavailable",
    "SessionError",
    "SessionExpired",
    "TokenExpired",
    "TransactionError",
    "TransactionNestingError",
    "TransientError",
    "UnsupportedServerProduct",
    "WriteServiceUnavailable",
]


if t.TYPE_CHECKING:
    from collections.abc import Mapping

    import typing_extensions as te

    from ._async.work import (
        AsyncManagedTransaction,
        AsyncResult,
        AsyncSession,
        AsyncTransaction,
    )
    from ._sync.work import (
        ManagedTransaction,
        Result,
        Session,
        Transaction,
    )

    _TTransaction = t.Union[
        AsyncManagedTransaction,
        AsyncTransaction,
        ManagedTransaction,
        Transaction,
    ]
    _TResult = t.Union[AsyncResult, Result]
    _TSession = t.Union[AsyncSession, Session]
    _T = t.TypeVar("_T")
else:
    _TTransaction = t.Union[
        "AsyncManagedTransaction",
        "AsyncTransaction",
        "ManagedTransaction",
        "Transaction",
    ]
    _TResult = t.Union["AsyncResult", "Result"]
    _TSession = t.Union["AsyncSession", "Session"]


__all__ = [
    "CLASSIFICATION_CLIENT",  # TODO: 6.0 - make constant private
    "CLASSIFICATION_DATABASE",  # TODO: 6.0 - make constant private
    "CLASSIFICATION_TRANSIENT",  # TODO: 6.0 - make constant private
    "ERROR_REWRITE_MAP",  # TODO: 6.0 - make constant private
    "AuthConfigurationError",
    "AuthError",
    "BrokenRecordError",
    "CertificateConfigurationError",
    "ClientError",
    "ConfigurationError",
    "ConstraintError",
    "CypherSyntaxError",
    "CypherTypeError",
    "DatabaseError",
    "DatabaseUnavailable",
    "DriverError",
    "Forbidden",
    "ForbiddenOnReadOnlyDatabase",
    "IncompleteCommit",
    "Neo4jError",
    "NotALeader",
    "ReadServiceUnavailable",
    "ResultConsumedError",
    "ResultError",
    "ResultFailedError",
    "ResultNotSingleError",
    "RoutingServiceUnavailable",
    "ServiceUnavailable",
    "SessionError",
    "SessionExpired",
    "TokenExpired",
    "TransactionError",
    "TransactionNestingError",
    "TransientError",
    "UnsupportedServerProduct",
    "WriteServiceUnavailable",
]


CLASSIFICATION_CLIENT: te.Final[str] = "ClientError"
CLASSIFICATION_TRANSIENT: te.Final[str] = "TransientError"
CLASSIFICATION_DATABASE: te.Final[str] = "DatabaseError"


ERROR_REWRITE_MAP: dict[str, tuple[str, str | None]] = {
    # This error can be retried ed. The driver just needs to re-authenticate
    # with the same credentials.
    "Neo.ClientError.Security.AuthorizationExpired": (
        CLASSIFICATION_TRANSIENT,
        None,
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.Terminated": (
        CLASSIFICATION_CLIENT,
        "Neo.ClientError.Transaction.Terminated",
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.LockClientStopped": (
        CLASSIFICATION_CLIENT,
        "Neo.ClientError.Transaction.LockClientStopped",
    ),
}


_UNKNOWN_NEO4J_CODE: te.Final[str] = "Neo.DatabaseError.General.UnknownError"
# TODO: 6.0 - Make _UNKNOWN_GQL_MESSAGE the default message
_UNKNOWN_MESSAGE: te.Final[str] = "An unknown error occurred"
_UNKNOWN_GQL_STATUS: te.Final[str] = "50N42"
_UNKNOWN_GQL_DESCRIPTION: te.Final[str] = (
    "error: general processing exception - unexpected error"
)
_UNKNOWN_GQL_MESSAGE: te.Final[str] = (
    f"{_UNKNOWN_GQL_STATUS}: "
    "Unexpected error has occurred. See debug log for details."
)
_UNKNOWN_GQL_DIAGNOSTIC_RECORD: te.Final[tuple[tuple[str, t.Any], ...]] = (
    ("OPERATION", ""),
    ("OPERATION_CODE", "0"),
    ("CURRENT_SCHEMA", "/"),
)


class GqlErrorClassification(str, _Enum):
    """
    Server-side GQL error category.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Hence, can also be compared to its string value::

        >>> GqlErrorClassification.CLIENT_ERROR == "CLIENT_ERROR"
        True
        >>> GqlErrorClassification.DATABASE_ERROR == "DATABASE_ERROR"
        True
        >>> GqlErrorClassification.TRANSIENT_ERROR == "TRANSIENT_ERROR"
        True

    **This is a preview**.
    It might be changed without following the deprecation policy.
    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :attr:`.GqlError.gql_classification`

    .. versionadded:: 5.26
    """

    CLIENT_ERROR = "CLIENT_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    TRANSIENT_ERROR = "TRANSIENT_ERROR"
    #: Used when the server provides a Classification which the driver is
    #: unaware of.
    #: This can happen when connecting to a server newer than the driver or
    #: before GQL errors were introduced.
    UNKNOWN = "UNKNOWN"


class GqlError(Exception):
    """
    The GQL compliant data of an error.

    This error isn't raised by the driver as is.
    Instead, only subclasses are raised.
    Further, it is used as the :attr:`__cause__` of GqlError subclasses.

    **This is a preview**.
    It might be changed without following the deprecation policy.
    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. versionadded: 5.26
    """

    _gql_status: str
    # TODO: 6.0 - make message always str
    _message: str | None
    _gql_status_description: str
    _gql_raw_classification: str | None
    _gql_classification: GqlErrorClassification
    _status_diagnostic_record: dict[str, t.Any]  # original, internal only
    _diagnostic_record: dict[str, t.Any]  # copy to be used externally
    _gql_cause: GqlError | None

    @staticmethod
    def _hydrate_cause(**metadata: t.Any) -> GqlError:
        meta_extractor = _MetaExtractor(metadata)
        gql_status = meta_extractor.str_value("gql_status")
        description = meta_extractor.str_value("description")
        message = meta_extractor.str_value("message")
        diagnostic_record = meta_extractor.map_value("diagnostic_record")
        cause_map = meta_extractor.map_value("cause")
        if cause_map is not None:
            cause = GqlError._hydrate_cause(**cause_map)
        else:
            cause = None
        inst = GqlError()
        inst._init_gql(
            gql_status=gql_status,
            message=message,
            description=description,
            diagnostic_record=diagnostic_record,
            cause=cause,
        )
        return inst

    def _init_gql(
        self,
        *,
        gql_status: str | None = None,
        message: str | None = None,
        description: str | None = None,
        diagnostic_record: dict[str, t.Any] | None = None,
        cause: GqlError | None = None,
    ) -> None:
        if gql_status is None or message is None or description is None:
            self._gql_status = _UNKNOWN_GQL_STATUS
            self._message = _UNKNOWN_GQL_MESSAGE
            self._gql_status_description = _UNKNOWN_GQL_DESCRIPTION
        else:
            self._gql_status = gql_status
            self._message = message
            self._gql_status_description = description
        if diagnostic_record is not None:
            self._status_diagnostic_record = diagnostic_record
        self._gql_cause = cause

    def _set_unknown_gql(self):
        self._gql_status = _UNKNOWN_GQL_STATUS
        self._message = _UNKNOWN_GQL_MESSAGE
        self._gql_status_description = _UNKNOWN_GQL_DESCRIPTION

    def __getattribute__(self, item):
        if item != "__cause__":
            return super().__getattribute__(item)
        gql_cause = self._get_attr_or_none("_gql_cause")
        if gql_cause is None:
            # No GQL cause, no magic needed
            return super().__getattribute__(item)
        local_cause = self._get_attr_or_none("__cause__")
        if local_cause is None:
            # We have a GQL cause but no local cause
            # => set the GQL cause as the local cause
            self.__cause__ = gql_cause
            self.__suppress_context__ = True
            self._gql_cause = None
            return super().__getattribute__(item)
        # We have both a GQL cause and a local cause
        # => traverse the cause chain and append the local cause.
        root = gql_cause
        seen_errors = {id(self), id(root)}
        while True:
            cause = getattr(root, "__cause__", None)
            if cause is None:
                root.__cause__ = local_cause
                root.__suppress_context__ = True
                self.__cause__ = gql_cause
                self.__suppress_context__ = True
                self._gql_cause = None
                return gql_cause
            root = cause
            if id(root) in seen_errors:
                # Circular cause chain -> we have no choice but to either
                # overwrite the cause or ignore the new one.
                return local_cause
            seen_errors.add(id(root))

    def _get_attr_or_none(self, item):
        try:
            return super().__getattribute__(item)
        except AttributeError:
            return None

    @property
    def _gql_status_no_preview(self) -> str:
        if hasattr(self, "_gql_status"):
            return self._gql_status

        self._set_unknown_gql()
        return self._gql_status

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def gql_status(self) -> str:
        """
        The GQLSTATUS returned from the server.

        The status code ``50N42`` (unknown error) is a special code that the
        driver will use for polyfilling (when connected to an old,
        non-GQL-aware server).
        Further, it may be used by servers during the transition-phase to
        GQLSTATUS-awareness.

        .. note::
            This means that the code ``50N42`` is not guaranteed to be stable
            and may change in future versions of the driver or the server.
        """
        return self._gql_status_no_preview

    @property
    def _message_no_preview(self) -> str | None:
        if hasattr(self, "_message"):
            return self._message

        self._set_unknown_gql()
        return self._message

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def message(self) -> str | None:
        """
        The error message returned by the server.

        It is a string representation of the error that occurred.

        This message is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.

        This value is never :data:`None` unless the subclass in question
        states otherwise.
        """
        return self._message_no_preview

    @property
    def _gql_status_description_no_preview(self) -> str:
        if hasattr(self, "_gql_status_description"):
            return self._gql_status_description

        self._set_unknown_gql()
        return self._gql_status_description

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def gql_status_description(self) -> str:
        """
        A description of the GQLSTATUS returned from the server.

        It describes the error that occurred in detail.

        This description is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.
        """
        return self._gql_status_description_no_preview

    @property
    def _gql_raw_classification_no_preview(self) -> str | None:
        if hasattr(self, "_gql_raw_classification"):
            return self._gql_raw_classification

        diag_record = self._get_status_diagnostic_record()
        classification = diag_record.get("_classification")
        if not isinstance(classification, str):
            self._gql_raw_classification = None
        else:
            self._gql_raw_classification = classification
        return self._gql_raw_classification

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def gql_raw_classification(self) -> str | None:
        """
        Vendor specific classification of the error.

        This is a convenience accessor for ``_classification`` in the
        diagnostic record.
        :data:`None` is returned if the classification is not available
        or not a string.
        """
        return self._gql_raw_classification_no_preview

    @property
    def _gql_classification_no_preview(self) -> GqlErrorClassification:
        if hasattr(self, "_gql_classification"):
            return self._gql_classification

        classification = self._gql_raw_classification_no_preview
        if not (
            isinstance(classification, str)
            and classification
            in t.cast(t.Iterable[str], iter(GqlErrorClassification))
        ):
            self._gql_classification = GqlErrorClassification.UNKNOWN
        else:
            self._gql_classification = GqlErrorClassification(classification)
        return self._gql_classification

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def gql_classification(self) -> GqlErrorClassification:
        return self._gql_classification_no_preview

    def _get_status_diagnostic_record(self) -> dict[str, t.Any]:
        if hasattr(self, "_status_diagnostic_record"):
            return self._status_diagnostic_record

        self._status_diagnostic_record = dict(_UNKNOWN_GQL_DIAGNOSTIC_RECORD)
        return self._status_diagnostic_record

    @property
    def _diagnostic_record_no_preview(self) -> Mapping[str, t.Any]:
        if hasattr(self, "_diagnostic_record"):
            return self._diagnostic_record

        self._diagnostic_record = _deepcopy(
            self._get_status_diagnostic_record()
        )
        return self._diagnostic_record

    @property
    @_preview("GQLSTATUS support is a preview feature.")
    def diagnostic_record(self) -> Mapping[str, t.Any]:
        return self._diagnostic_record_no_preview

    def __str__(self):
        return (
            f"{{gql_status: {self._gql_status_no_preview}}} "
            f"{{gql_status_description: "
            f"{self._gql_status_description_no_preview}}} "
            f"{{message: {self._message_no_preview}}} "
            f"{{diagnostic_record: {self._diagnostic_record_no_preview}}} "
            f"{{raw_classification: "
            f"{self._gql_raw_classification_no_preview}}}"
        )


# Neo4jError
class Neo4jError(GqlError):
    """Raised when the Cypher engine returns an error to the client."""

    _neo4j_code: str | None
    _classification: str | None
    _category: str | None
    _title: str | None
    #: (dict) Any additional information returned by the server.
    _metadata: dict[str, t.Any] | None

    _retryable = False

    def __init__(self, *args) -> None:
        Exception.__init__(self, *args)
        self._neo4j_code = None
        self._classification = None
        self._category = None
        self._title = None
        self._metadata = None
        self._message = None

        # TODO: 6.0 - do this instead to get rid of all optional attributes
        # self._neo4j_code = _UNKNOWN_NEO4J_CODE
        # _, self._classification, self._category, self._title = (
        #     self._neo4j_code.split(".")
        # )
        # self._metadata = {}
        # self._init_gql()

    # TODO: 6.0 - Remove this alias
    @classmethod
    @deprecated(
        "Neo4jError.hydrate is deprecated and will be removed in a future "
        "version. It is an internal method and not meant for external use."
    )
    def hydrate(
        cls,
        code: str | None = None,
        message: str | None = None,
        **metadata: t.Any,
    ) -> Neo4jError:
        # backward compatibility: make falsy values None
        code = code or None
        message = message or None
        return cls._hydrate_neo4j(code=code, message=message, **metadata)

    @classmethod
    def _hydrate_neo4j(cls, **metadata: t.Any) -> Neo4jError:
        meta_extractor = _MetaExtractor(metadata)
        code = meta_extractor.str_value("code") or _UNKNOWN_NEO4J_CODE
        message = meta_extractor.str_value("message") or _UNKNOWN_MESSAGE
        inst = cls._basic_hydrate(
            neo4j_code=code,
            message=message,
        )
        inst._init_gql(
            gql_status=_UNKNOWN_GQL_STATUS,
            message=message,
            description=f"{_UNKNOWN_GQL_DESCRIPTION}. {message}",
        )
        inst._metadata = meta_extractor.rest()
        return inst

    @classmethod
    def _hydrate_gql(cls, **metadata: t.Any) -> Neo4jError:
        meta_extractor = _MetaExtractor(metadata)
        gql_status = meta_extractor.str_value("gql_status")
        status_description = meta_extractor.str_value("description")
        message = meta_extractor.str_value("message")
        if gql_status is None or status_description is None or message is None:
            gql_status = _UNKNOWN_GQL_STATUS
            # TODO: 6.0 - Make this fall back to _UNKNOWN_GQL_MESSAGE
            message = _UNKNOWN_MESSAGE
            status_description = _UNKNOWN_GQL_DESCRIPTION
        neo4j_code = meta_extractor.str_value(
            "neo4j_code",
            _UNKNOWN_NEO4J_CODE,
        )
        diagnostic_record = meta_extractor.map_value("diagnostic_record")
        cause_map = meta_extractor.map_value("cause")
        if cause_map is not None:
            cause = cls._hydrate_cause(**cause_map)
        else:
            cause = None

        inst = cls._basic_hydrate(
            neo4j_code=neo4j_code,
            message=message,
        )
        inst._init_gql(
            gql_status=gql_status,
            message=message,
            description=status_description,
            diagnostic_record=diagnostic_record,
            cause=cause,
        )
        inst._metadata = meta_extractor.rest()

        return inst

    @classmethod
    def _basic_hydrate(cls, *, neo4j_code: str, message: str) -> Neo4jError:
        try:
            _, classification, category, title = neo4j_code.split(".")
        except ValueError:
            classification = CLASSIFICATION_DATABASE
            category = "General"
            title = "UnknownError"
        else:
            classification_override, code_override = ERROR_REWRITE_MAP.get(
                neo4j_code, (None, None)
            )
            if classification_override is not None:
                classification = classification_override
            if code_override is not None:
                neo4j_code = code_override

        error_class: type[Neo4jError] = cls._extract_error_class(
            classification, neo4j_code
        )

        inst = error_class(message)
        inst._neo4j_code = neo4j_code
        inst._classification = classification
        inst._category = category
        inst._title = title
        inst._message = message

        return inst

    @classmethod
    def _extract_error_class(cls, classification, code) -> type[Neo4jError]:
        if classification == CLASSIFICATION_CLIENT:
            try:
                return client_errors[code]
            except KeyError:
                return ClientError

        elif classification == CLASSIFICATION_TRANSIENT:
            try:
                return transient_errors[code]
            except KeyError:
                return TransientError

        elif classification == CLASSIFICATION_DATABASE:
            return DatabaseError

        else:
            return cls

    @property
    def message(self) -> str | None:
        """
        The error message returned by the server.

        This value is only :data:`None` for locally created errors.
        """
        return self._message

    @message.setter
    @deprecated("Altering the message of a Neo4jError is deprecated.")
    def message(self, value: str) -> None:
        self._message = value

    @property
    def code(self) -> str | None:
        """
        The neo4j error code returned by the server.

        For example, "Neo.ClientError.Security.AuthorizationExpired".
        This value is only :data:`None` for locally created errors.
        """
        return self._neo4j_code

    # TODO: 6.0 - Remove this and all other deprecated setters
    @code.setter
    @deprecated("Altering the code of a Neo4jError is deprecated.")
    def code(self, value: str) -> None:
        self._neo4j_code = value

    @property
    def classification(self) -> str | None:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._classification

    @classification.setter
    @deprecated("Altering the classification of Neo4jError is deprecated.")
    def classification(self, value: str) -> None:
        self._classification = value

    @property
    def category(self) -> str | None:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._category

    @category.setter
    @deprecated("Altering the category of Neo4jError is deprecated.")
    def category(self, value: str) -> None:
        self._category = value

    @property
    def title(self) -> str | None:
        # Undocumented, will likely be removed with support for neo4j codes
        return self._title

    @title.setter
    @deprecated("Altering the title of Neo4jError is deprecated.")
    def title(self, value: str) -> None:
        self._title = value

    @property
    def metadata(self) -> dict[str, t.Any] | None:
        # Undocumented, might be useful for debugging
        return self._metadata

    @metadata.setter
    @deprecated("Altering the metadata of Neo4jError is deprecated.")
    def metadata(self, value: dict[str, t.Any]) -> None:
        self._metadata = value

    # TODO: 6.0 - Remove this alias
    @deprecated(
        "Neo4jError.is_retriable is deprecated and will be removed in a "
        "future version. Please use Neo4jError.is_retryable instead."
    )
    def is_retriable(self) -> bool:
        """
        Whether the error is retryable.

        See :meth:`.is_retryable`.

        :returns: :data:`True` if the error is retryable,
            :data:`False` otherwise.

        .. deprecated:: 5.0
            This method will be removed in a future version.
            Please use :meth:`.is_retryable` instead.
        """
        return self.is_retryable()

    def is_retryable(self) -> bool:
        """
        Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        :returns: :data:`True` if the error is retryable,
            :data:`False` otherwise.

        .. versionadded:: 5.0
        """
        return self._retryable

    def _unauthenticates_all_connections(self) -> bool:
        return (
            self._neo4j_code == "Neo.ClientError.Security.AuthorizationExpired"
        )

    # TODO: 6.0 - Remove this alias
    invalidates_all_connections = deprecated(
        "Neo4jError.invalidates_all_connections is deprecated and will be "
        "removed in a future version. It is an internal method and not meant "
        "for external use."
    )(_unauthenticates_all_connections)

    def _is_fatal_during_discovery(self) -> bool:
        # checks if the code is an error that is caused by the client. In this
        # case the driver should fail fast during discovery.
        code = self._neo4j_code
        if not isinstance(code, str):
            return False
        if code in {
            "Neo.ClientError.Database.DatabaseNotFound",
            "Neo.ClientError.Transaction.InvalidBookmark",
            "Neo.ClientError.Transaction.InvalidBookmarkMixture",
            "Neo.ClientError.Statement.TypeError",
            "Neo.ClientError.Statement.ArgumentError",
            "Neo.ClientError.Request.Invalid",
        }:
            return True
        return (
            code.startswith("Neo.ClientError.Security.")
            and code != "Neo.ClientError.Security.AuthorizationExpired"
        )

    def _has_security_code(self) -> bool:
        if self._neo4j_code is None:
            return False
        return self._neo4j_code.startswith("Neo.ClientError.Security.")

    # TODO: 6.0 - Remove this alias
    is_fatal_during_discovery = deprecated(
        "Neo4jError.is_fatal_during_discovery is deprecated and will be "
        "removed in a future version. It is an internal method and not meant "
        "for external use."
    )(_is_fatal_during_discovery)

    def __str__(self):
        code = self._neo4j_code
        message = self._message
        if code or message:
            return f"{{code: {code}}} {{message: {message}}}"
            # TODO: 6.0 - Use gql status and status_description instead
            # something like:
            # return (
            #     f"{{gql_status: {self.gql_status}}} "
            #     f"{{neo4j_code: {self.neo4j_code}}} "
            #     f"{{gql_status_description: {self.gql_status_description}}} "
            #     f"{{diagnostic_record: {self.diagnostic_record}}}"
            # )
        return Exception.__str__(self)


class _MetaExtractor:
    def __init__(self, metadata: dict[str, t.Any]):
        self._metadata = metadata

    def rest(self) -> dict[str, t.Any]:
        return self._metadata

    @t.overload
    def str_value(self, key: str) -> str | None: ...

    @t.overload
    def str_value(self, key: str, default: _T) -> str | _T: ...

    def str_value(
        self, key: str, default: _T | None = None
    ) -> str | _T | None:
        res = self._metadata.pop(key, default)
        if not isinstance(res, str):
            res = default
        return res

    @t.overload
    def map_value(self, key: str) -> dict[str, t.Any] | None: ...

    @t.overload
    def map_value(self, key: str, default: _T) -> dict[str, t.Any] | _T: ...

    def map_value(
        self, key: str, default: _T | None = None
    ) -> dict[str, t.Any] | _T | None:
        res = self._metadata.pop(key, default)
        if not (
            isinstance(res, dict) and all(isinstance(k, str) for k in res)
        ):
            res = default
        return res


# Neo4jError > ClientError
class ClientError(Neo4jError):
    """
    Bad client request.

    The Client sent a bad request - changing the request might yield a
    successful outcome.
    """


# Neo4jError > ClientError > CypherSyntaxError
class CypherSyntaxError(ClientError):
    pass


# Neo4jError > ClientError > CypherTypeError
class CypherTypeError(ClientError):
    pass


# Neo4jError > ClientError > ConstraintError
class ConstraintError(ClientError):
    pass


# Neo4jError > ClientError > AuthError
class AuthError(ClientError):
    """Raised when authentication failure occurs."""


# Neo4jError > ClientError > AuthError > TokenExpired
class TokenExpired(AuthError):
    """Raised when the authentication token has expired."""


# Neo4jError > ClientError > Forbidden
class Forbidden(ClientError):
    pass


# Neo4jError > DatabaseError
class DatabaseError(Neo4jError):
    """The database failed to service the request."""


# Neo4jError > TransientError
class TransientError(Neo4jError):
    """
    Transient Error.

    The database cannot service the request right now, retrying later might
    yield a successful outcome.
    """

    _retryable = True


# Neo4jError > TransientError > DatabaseUnavailable
class DatabaseUnavailable(TransientError):
    pass


# Neo4jError > TransientError > NotALeader
class NotALeader(TransientError):
    pass


# Neo4jError > TransientError > ForbiddenOnReadOnlyDatabase
class ForbiddenOnReadOnlyDatabase(TransientError):
    pass


# TODO: 6.0 - Make map private
client_errors: dict[str, type[Neo4jError]] = {
    # ConstraintError
    "Neo.ClientError.Schema.ConstraintValidationFailed": ConstraintError,
    "Neo.ClientError.Schema.ConstraintViolation": ConstraintError,
    "Neo.ClientError.Statement.ConstraintVerificationFailed": ConstraintError,
    "Neo.ClientError.Statement.ConstraintViolation": ConstraintError,
    # CypherSyntaxError
    "Neo.ClientError.Statement.InvalidSyntax": CypherSyntaxError,
    "Neo.ClientError.Statement.SyntaxError": CypherSyntaxError,
    # CypherTypeError
    "Neo.ClientError.Procedure.TypeError": CypherTypeError,
    "Neo.ClientError.Statement.InvalidType": CypherTypeError,
    "Neo.ClientError.Statement.TypeError": CypherTypeError,
    # Forbidden
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": ForbiddenOnReadOnlyDatabase,  # noqa: E501
    "Neo.ClientError.General.ReadOnly": Forbidden,
    "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
    "Neo.ClientError.Schema.IndexBelongsToConstraint": Forbidden,
    "Neo.ClientError.Security.Forbidden": Forbidden,
    "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,
    # AuthError
    "Neo.ClientError.Security.AuthorizationFailed": AuthError,
    "Neo.ClientError.Security.Unauthorized": AuthError,
    # TokenExpired
    "Neo.ClientError.Security.TokenExpired": TokenExpired,
    # NotALeader
    "Neo.ClientError.Cluster.NotALeader": NotALeader,
}

# TODO: 6.0 - Make map private
transient_errors: dict[str, type[Neo4jError]] = {
    # DatabaseUnavailableError
    "Neo.TransientError.General.DatabaseUnavailable": DatabaseUnavailable
}


# DriverError
class DriverError(GqlError):
    """Raised when the Driver raises an error."""

    def is_retryable(self) -> bool:
        """
        Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        :returns: :data:`True` if the error is retryable,
            :data:`False` otherwise.

        .. versionadded:: 5.0
        """
        return False

    def __str__(self):
        return Exception.__str__(self)


# DriverError > SessionError
class SessionError(DriverError):
    """Raised when an error occurs while using a session."""

    session: _TSession

    def __init__(self, session_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session_


# DriverError > TransactionError
class TransactionError(DriverError):
    """Raised when an error occurs while using a transaction."""

    transaction: _TTransaction

    def __init__(self, transaction_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transaction = transaction_


# DriverError > TransactionError > TransactionNestingError
class TransactionNestingError(TransactionError):
    """Raised when transactions are nested incorrectly."""


# DriverError > ResultError
class ResultError(DriverError):
    """Raised when an error occurs while using a result object."""

    result: _TResult

    def __init__(self, result_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = result_


# DriverError > ResultError > ResultFailedError
class ResultFailedError(ResultError):
    """
    Raised when trying to access records of a failed result.

    A :class:`.Result` will be considered failed if
     * itself encountered an error while fetching records
     * another result within the same transaction encountered an error while
       fetching records
    """


# DriverError > ResultError > ResultConsumedError
class ResultConsumedError(ResultError):
    """Raised when trying to access records of a consumed result."""


# DriverError > ResultError > ResultNotSingleError
class ResultNotSingleError(ResultError):
    """Raised when a result should have exactly one record but does not."""


# DriverError > BrokenRecordError
class BrokenRecordError(DriverError):
    """
    Raised when accessing a Record's field that couldn't be decoded.

    This can for instance happen when the server sends a zoned datetime with a
    zone id unknown to the client.
    """


# DriverError > SessionExpired
class SessionExpired(DriverError):
    """
    The session has expired.

    Raised when a session is no longer able to fulfil the purpose described by
    its original parameters.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self._init_gql(
            gql_status="08000",
            description="error: connection exception",
        )

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable
class ServiceUnavailable(DriverError):
    """
    Raised when no database service is available.

    This may be due to incorrect configuration or could indicate a runtime
    failure of a database service that the driver is unable to route around.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self._init_gql(
            gql_status="08000",
            description="error: connection exception",
        )

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable > RoutingServiceUnavailable
class RoutingServiceUnavailable(ServiceUnavailable):
    """Raised when no routing service is available."""


# DriverError > ServiceUnavailable > WriteServiceUnavailable
class WriteServiceUnavailable(ServiceUnavailable):
    """Raised when no write service is available."""


# DriverError > ServiceUnavailable > ReadServiceUnavailable
class ReadServiceUnavailable(ServiceUnavailable):
    """Raised when no read service is available."""


# DriverError > ServiceUnavailable > IncompleteCommit
class IncompleteCommit(ServiceUnavailable):
    """
    Raised when the client looses connection while committing a transaction.

    Raised when a disconnection occurs while still waiting for a commit
    response. For non-idempotent write transactions, this leaves the data
    in an unknown state with regard to whether the transaction completed
    successfully or not.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self._init_gql(
            gql_status="08007",
            description=(
                "error: connection exception - "
                "transaction resolution unknown"
            ),
        )

    def is_retryable(self) -> bool:
        return False


# DriverError > ConfigurationError
class ConfigurationError(DriverError):
    """Raised when there is an error concerning a configuration."""


# DriverError > ConfigurationError > AuthConfigurationError
class AuthConfigurationError(ConfigurationError):
    """Raised when there is an error with the authentication configuration."""


# DriverError > ConfigurationError > CertificateConfigurationError
class CertificateConfigurationError(ConfigurationError):
    """Raised when there is an error with the certificate configuration."""


class UnsupportedServerProduct(Exception):
    """Raised when an unsupported server product is detected."""
