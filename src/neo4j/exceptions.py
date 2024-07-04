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

import sys
import typing as t
from copy import deepcopy

from ._meta import (
    deprecated,
    preview,
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


_UNKNOWN_GQL_STATUS = "50N42"
_UNKNOWN_GQL_EXPLANATION = "general processing exception - unknown error"
_UNKNOWN_GQL_CLASSIFICATION = "UNKNOWN"  # TODO: define final value
_UNKNOWN_GQL_DIAGNOSTIC_RECORD = (
    (
        "OPERATION",
        "",
    ),
    (
        "OPERATION_CODE",
        "0",
    ),
    ("CURRENT_SCHEMA", "/"),
)


def _make_gql_description(explanation: str, message: str | None = None) -> str:
    if message is None:
        return f"error: {explanation}"

    return f"error: {explanation}. {message}"


# Neo4jError
class Neo4jError(Exception):
    """Raised when the Cypher engine returns an error to the client."""

    _neo4j_code: str
    _message: str
    _classification: str
    _category: str
    _title: str
    #: (dict) Any additional information returned by the server.
    _metadata: dict[str, t.Any]

    _gql_status: str
    _gql_explanation: str  # internal use only
    _gql_description: str
    _gql_status_description: str
    _gql_classification: str
    _status_diagnostic_record: dict[str, t.Any]  # original, internal only
    _diagnostic_record: dict[str, t.Any]  # copy to be used externally

    _retryable = False

    @classmethod
    def _hydrate(
        cls,
        *,
        neo4j_code: str | None = None,
        message: str | None = None,
        gql_status: str | None = None,
        explanation: str | None = None,
        diagnostic_record: dict[str, t.Any] | None = None,
        cause: Neo4jError | None = None,
    ) -> te.Self:
        neo4j_code = neo4j_code or "Neo.DatabaseError.General.UnknownError"
        message = message or "An unknown error occurred"

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

        error_class = cls._extract_error_class(classification, neo4j_code)

        if explanation is not None:
            gql_description = _make_gql_description(explanation, message)
            inst = error_class(gql_description)
            inst._gql_description = gql_description
        else:
            inst = error_class(message)

        inst._neo4j_code = neo4j_code
        inst._message = message
        inst._classification = classification
        inst._category = category
        inst._title = title
        if gql_status:
            inst._gql_status = gql_status
        if explanation:
            inst._gql_explanation = explanation
        if diagnostic_record is not None:
            inst._status_diagnostic_record = diagnostic_record
        if cause:
            inst.__cause__ = cause
        else:
            current_exc = sys.exc_info()[1]
            if current_exc is not None:
                inst.__context__ = current_exc

        return inst

    @classmethod
    def _extract_error_class(cls, classification, code):
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

    @classmethod
    def hydrate(
        cls,
        code: str | None = None,
        message: str | None = None,
        **metadata: t.Any,
    ) -> te.Self:
        inst = cls._hydrate(neo4j_code=code, message=message)
        inst._metadata = metadata
        return inst

    @classmethod
    def _hydrate_gql(cls, **metadata: t.Any) -> te.Self:
        gql_status = metadata.pop("gql_status", None)
        if not isinstance(gql_status, str):
            gql_status = None
        status_explanation = metadata.pop("status_explanation", None)
        if not isinstance(status_explanation, str):
            status_explanation = None
        message = metadata.pop("status_message", None)
        if not isinstance(message, str):
            message = None
        neo4j_code = metadata.pop("neo4j_code", None)
        if not isinstance(neo4j_code, str):
            neo4j_code = None
        diagnostic_record = metadata.pop("diagnostic_record", None)
        if not isinstance(diagnostic_record, dict):
            diagnostic_record = None
        cause = metadata.pop("cause", None)
        if not isinstance(cause, dict):
            cause = None
        else:
            cause = cls._hydrate_gql(**cause)

        inst = cls._hydrate(
            neo4j_code=neo4j_code,
            message=message,
            gql_status=gql_status,
            explanation=status_explanation,
            diagnostic_record=diagnostic_record,
            cause=cause,
        )
        inst._metadata = metadata

        return inst

    @property
    def message(self) -> str:
        """
        TODO.

        #: (str or None) The error message returned by the server.
        """
        return self._message

    @message.setter
    @deprecated("Altering the message of a Neo4jError is deprecated.")
    def message(self, value: str) -> None:
        self._message = value

    # TODO: 6.0 - Remove this alias
    @property
    @deprecated(
        "The code of a Neo4jError is deprecated. Use neo4j_code instead."
    )
    def code(self) -> str:
        """
        The neo4j error code returned by the server.

        .. deprecated:: 5.xx
            Use :attr:`.neo4j_code` instead.
        """
        return self._neo4j_code

    # TODO: 6.0 - Remove this and all other deprecated setters
    @code.setter
    @deprecated("Altering the code of a Neo4jError is deprecated.")
    def code(self, value: str) -> None:
        self._neo4j_code = value

    @property
    def neo4j_code(self) -> str:
        """
        The error code returned by the server.

        There are many Neo4j status codes, see
        `status codes <https://neo4j.com/docs/status-codes/current/>`_.

        .. versionadded: 5.xx
        """
        return self._neo4j_code

    @property
    @deprecated("classification of Neo4jError is deprecated.")
    def classification(self) -> str:
        # Undocumented, has been there before
        # TODO 6.0: Remove this property
        return self._classification

    @classification.setter
    @deprecated("classification of Neo4jError is deprecated.")
    def classification(self, value: str) -> None:
        self._classification = value

    @property
    @deprecated("category of Neo4jError is deprecated.")
    def category(self) -> str:
        # Undocumented, has been there before
        # TODO 6.0: Remove this property
        return self._category

    @category.setter
    @deprecated("category of Neo4jError is deprecated.")
    def category(self, value: str) -> None:
        self._category = value

    @property
    @deprecated("title of Neo4jError is deprecated.")
    def title(self) -> str:
        # Undocumented, has been there before
        # TODO 6.0: Remove this property
        return self._title

    @title.setter
    @deprecated("title of Neo4jError is deprecated.")
    def title(self, value: str) -> None:
        self._title = value

    @property
    def metadata(self) -> dict[str, t.Any]:
        # Undocumented, might be useful for debugging
        return self._metadata

    @metadata.setter
    @deprecated("Altering the metadata of Neo4jError is deprecated.")
    def metadata(self, value: dict[str, t.Any]) -> None:
        # TODO 6.0: Remove this property
        self._metadata = value

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_status(self) -> str:
        """
        The GQLSTATUS returned from the server.

        The status code ``50N42`` (unknown error) is a special code that the
        driver will use for polyfilling (when connected to an old,
        non-GQL-aware server).
        Further, it may be used by servers during the transition-phase to
        GQLSTATUS-awareness.

        This means this code is not guaranteed to be stable and may change in
        future versions.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        if hasattr(self, "_gql_status"):
            return self._gql_status

        self._gql_status = _UNKNOWN_GQL_STATUS
        return self._gql_status

    def _get_explanation(self) -> str:
        if hasattr(self, "_gql_explanation"):
            return self._gql_explanation

        self._gql_explanation = _UNKNOWN_GQL_EXPLANATION
        return self._gql_explanation

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_status_description(self) -> str:
        """
        A description of the GQLSTATUS returned from the server.

        This description is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        if hasattr(self, "_gql_status_description"):
            return self._gql_status_description

        self._gql_status_description = _make_gql_description(
            self._get_explanation(), self._message
        )
        return self._gql_status_description

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_classification(self) -> str:
        """
        TODO.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        # TODO
        if hasattr(self, "_gql_classification"):
            return self._gql_classification

        diag_record = self._get_status_diagnostic_record()
        classification = diag_record.get("_classification")
        if not isinstance(classification, str):
            self._classification = _UNKNOWN_GQL_CLASSIFICATION
        else:
            self._classification = classification
        return self._classification

    def _get_status_diagnostic_record(self) -> dict[str, t.Any]:
        if hasattr(self, "_status_diagnostic_record"):
            return self._status_diagnostic_record

        self._status_diagnostic_record = dict(_UNKNOWN_GQL_DIAGNOSTIC_RECORD)
        return self._status_diagnostic_record

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def diagnostic_record(self) -> dict[str, t.Any]:
        """
        Further information about the GQLSTATUS for diagnostic purposes.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        if hasattr(self, "_diagnostic_record"):
            return self._diagnostic_record

        self._diagnostic_record = deepcopy(
            self._get_status_diagnostic_record()
        )
        return self._diagnostic_record

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
            return f"{{neo4j_code: {code}}} {{message: {message}}}"
            # TODO: 6.0 - User gql status and status_description instead
            # something like:
            # return (
            #     f"{{gql_status: {self.gql_status}}} "
            #     f"{{neo4j_code: {self.neo4j_code}}} "
            #     f"{{gql_status_description: {self.gql_status_description}}} "
            #     f"{{diagnostic_record: {self.diagnostic_record}}}"
            # )
        return super().__str__()


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
class DriverError(Exception):
    """Raised when the Driver raises an error."""

    _diagnostic_record: dict[str, t.Any]
    _gql_description: str

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

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_status(self) -> str:
        """
        The GQLSTATUS of this error.

        .. seealso:: :attr:`.Neo4jError.gql_status`

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        return _UNKNOWN_GQL_STATUS

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_status_description(self) -> str:
        """
        A description of the GQLSTATUS.

        This description is meant for human consumption and debugging purposes.
        Don't rely on it in a programmatic way.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        if hasattr(self, "_gql_description"):
            return self._gql_description

        self._gql_description = _make_gql_description(_UNKNOWN_GQL_EXPLANATION)
        return self._gql_description

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def gql_classification(self) -> str:
        """
        TODO.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        return _UNKNOWN_GQL_CLASSIFICATION

    @property
    @preview("GQLSTATUS support is a preview feature.")
    def diagnostic_record(self) -> dict[str, t.Any]:
        """
        Further information about the GQLSTATUS for diagnostic purposes.

        **This is a preview**.
        It might be changed without following the deprecation policy.
        See also
        https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

        .. versionadded: 5.xx
        """
        if hasattr(self, "_diagnostic_record"):
            return self._diagnostic_record

        self._diagnostic_record = dict(_UNKNOWN_GQL_DIAGNOSTIC_RECORD)
        return self._diagnostic_record


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

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable
class ServiceUnavailable(DriverError):
    """
    Raised when no database service is available.

    This may be due to incorrect configuration or could indicate a runtime
    failure of a database service that the driver is unable to route around.
    """

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
