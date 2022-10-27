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


"""
This module contains the core driver exceptions.

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

Connector API Errors
====================
+ BoltError
  + BoltHandshakeError
  + BoltRoutingError
  + BoltConnectionError
      + BoltSecurityError
      + BoltConnectionBroken
      + BoltConnectionClosed
  + BoltFailure
  + BoltProtocolError
  + Bolt*
"""


from __future__ import annotations

import typing as t


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

    _T_Transaction = t.Union[AsyncManagedTransaction, AsyncTransaction,
                             ManagedTransaction, Transaction]
    _T_Result = t.Union[AsyncResult, Result]
    _T_Session = t.Union[AsyncSession, Session]
else:
    _T_Transaction = t.Union["AsyncManagedTransaction", "AsyncTransaction",
                             "ManagedTransaction", "Transaction"]
    _T_Result = t.Union["AsyncResult", "Result"]
    _T_Session = t.Union["AsyncSession", "Session"]


from ._meta import deprecated


CLASSIFICATION_CLIENT: te.Final[str] = "ClientError"
CLASSIFICATION_TRANSIENT: te.Final[str] = "TransientError"
CLASSIFICATION_DATABASE: te.Final[str] = "DatabaseError"


ERROR_REWRITE_MAP: t.Dict[str, t.Tuple[str, t.Optional[str]]] = {
    # This error can be retried ed. The driver just needs to re-authenticate
    # with the same credentials.
    "Neo.ClientError.Security.AuthorizationExpired": (
        CLASSIFICATION_TRANSIENT, None
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.Terminated": (
        CLASSIFICATION_CLIENT, "Neo.ClientError.Transaction.Terminated"
    ),
    # In 5.0, this error has been re-classified as ClientError.
    # For backwards compatibility with Neo4j 4.4 and earlier, we re-map it in
    # the driver, too.
    "Neo.TransientError.Transaction.LockClientStopped": (
        CLASSIFICATION_CLIENT, "Neo.ClientError.Transaction.LockClientStopped"
    ),
}


# Neo4jError
class Neo4jError(Exception):
    """ Raised when the Cypher engine returns an error to the client.
    """

    #: (str or None) The error message returned by the server.
    message = None
    #: (str or None) The error code returned by the server.
    #: There are many Neo4j status codes, see
    #: `status codes <https://neo4j.com/docs/status-codes/current/>`_.
    code = None
    classification = None
    category = None
    title = None
    #: (dict) Any additional information returned by the server.
    metadata = None

    @classmethod
    def hydrate(
        cls, message: str = None, code: str = None, **metadata: t.Any
    ) -> Neo4jError:
        message = message or "An unknown error occurred"
        code = code or "Neo.DatabaseError.General.UnknownError"
        try:
            _, classification, category, title = code.split(".")
        except ValueError:
            classification = CLASSIFICATION_DATABASE
            category = "General"
            title = "UnknownError"
        else:
            classification_override, code_override = \
                ERROR_REWRITE_MAP.get(code, (None, None))
            if classification_override is not None:
                classification = classification_override
            if code_override is not None:
                code = code_override

        error_class = cls._extract_error_class(classification, code)

        inst = error_class(message)
        inst.message = message
        inst.code = code
        inst.classification = classification
        inst.category = category
        inst.title = title
        inst.metadata = metadata
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

    # TODO 6.0: Remove this alias
    @deprecated(
        "Neo4jError.is_retriable is deprecated and will be removed in a "
        "future version. Please use Neo4jError.is_retryable instead."
    )
    def is_retriable(self) -> bool:
        """Whether the error is retryable.

        See :meth:`.is_retryable`.

        :return: :const:`True` if the error is retryable,
            :const:`False` otherwise.

        .. deprecated:: 5.0
            This method will be removed in a future version.
            Please use :meth:`.is_retryable` instead.
        """
        return self.is_retryable()

    def is_retryable(self) -> bool:
        """Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        :return: :const:`True` if the error is retryable,
            :const:`False` otherwise.
        """
        return False

    def invalidates_all_connections(self):
        return self.code == "Neo.ClientError.Security.AuthorizationExpired"

    def is_fatal_during_discovery(self) -> bool:
        # checks if the code is an error that is caused by the client. In this
        # case the driver should fail fast during discovery.
        if not isinstance(self.code, str):
            return False
        if self.code in ("Neo.ClientError.Database.DatabaseNotFound",
                         "Neo.ClientError.Transaction.InvalidBookmark",
                         "Neo.ClientError.Transaction.InvalidBookmarkMixture"):
            return True
        if (self.code.startswith("Neo.ClientError.Security.")
                and self.code != "Neo.ClientError.Security."
                                 "AuthorizationExpired"):
            return True
        return False

    def __str__(self):
        if self.code or self.message:
            return "{{code: {code}}} {{message: {message}}}".format(
                code=self.code, message=self.message
            )
        return super().__str__()


# Neo4jError > ClientError
class ClientError(Neo4jError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """


# Neo4jError > ClientError > CypherSyntaxError
class CypherSyntaxError(ClientError):
    """
    """


# Neo4jError > ClientError > CypherTypeError
class CypherTypeError(ClientError):
    """
    """


# Neo4jError > ClientError > ConstraintError
class ConstraintError(ClientError):
    """
    """


# Neo4jError > ClientError > AuthError
class AuthError(ClientError):
    """ Raised when authentication failure occurs.
    """


# Neo4jError > ClientError > AuthError > TokenExpired
class TokenExpired(AuthError):
    """ Raised when the authentication token has expired.

    A new driver instance with a fresh authentication token needs to be created.
    """


# Neo4jError > ClientError > Forbidden
class Forbidden(ClientError):
    """
    """


# Neo4jError > DatabaseError
class DatabaseError(Neo4jError):
    """ The database failed to service the request.
    """


# Neo4jError > TransientError
class TransientError(Neo4jError):
    """ The database cannot service the request right now, retrying later might yield a successful outcome.
    """

    def is_retryable(self) -> bool:
        return True


# Neo4jError > TransientError > DatabaseUnavailable
class DatabaseUnavailable(TransientError):
    """
    """


# Neo4jError > TransientError > NotALeader
class NotALeader(TransientError):
    """
    """


# Neo4jError > TransientError > ForbiddenOnReadOnlyDatabase
class ForbiddenOnReadOnlyDatabase(TransientError):
    """
    """


client_errors: t.Dict[str, t.Type[Neo4jError]] = {

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
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": ForbiddenOnReadOnlyDatabase,
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

transient_errors: t.Dict[str, t.Type[Neo4jError]] = {

    # DatabaseUnavailableError
    "Neo.TransientError.General.DatabaseUnavailable": DatabaseUnavailable
}


# DriverError
class DriverError(Exception):
    """ Raised when the Driver raises an error.
    """
    def is_retryable(self) -> bool:
        """Whether the error is retryable.

        Indicates whether a transaction that yielded this error makes sense to
        retry. This method makes mostly sense when implementing a custom
        retry policy in conjunction with :ref:`explicit-transactions-ref`.

        :return: :const:`True` if the error is retryable,
            :const:`False` otherwise.
        """
        return False


# DriverError > SessionError
class SessionError(DriverError):
    """ Raised when an error occurs while using a session.
    """

    session: _T_Session

    def __init__(self, session_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session_


# DriverError > TransactionError
class TransactionError(DriverError):
    """ Raised when an error occurs while using a transaction.
    """

    transaction: _T_Transaction

    def __init__(self, transaction_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transaction = transaction_


# DriverError > TransactionError > TransactionNestingError
class TransactionNestingError(TransactionError):
    """ Raised when transactions are nested incorrectly.
    """


# DriverError > ResultError
class ResultError(DriverError):
    """Raised when an error occurs while using a result object."""

    result: _T_Result

    def __init__(self, result_, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = result_


# DriverError > ResultError > ResultConsumedError
class ResultConsumedError(ResultError):
    """Raised when trying to access records of a consumed result."""


# DriverError > ResultError > ResultNotSingleError
class ResultNotSingleError(ResultError):
    """Raised when a result should have exactly one record but does not."""


# DriverError > BrokenRecordError
class BrokenRecordError(DriverError):
    """ Raised when accessing a Record's field that couldn't be decoded.

    This can for instance happen when the server sends a zoned datetime with a
    zone id unknown to the client.
    """


# DriverError > SessionExpired
class SessionExpired(DriverError):
    """ Raised when a session is no longer able to fulfil
    the purpose described by its original parameters.
    """

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable
class ServiceUnavailable(DriverError):
    """ Raised when no database service is available.

    This may be due to incorrect configuration or could indicate a runtime
    failure of a database service that the driver is unable to route around.
    """

    def is_retryable(self) -> bool:
        return True


# DriverError > ServiceUnavailable > RoutingServiceUnavailable
class RoutingServiceUnavailable(ServiceUnavailable):
    """ Raised when no routing service is available.
    """


# DriverError > ServiceUnavailable > WriteServiceUnavailable
class WriteServiceUnavailable(ServiceUnavailable):
    """ Raised when no write service is available.
    """


# DriverError > ServiceUnavailable > ReadServiceUnavailable
class ReadServiceUnavailable(ServiceUnavailable):
    """ Raised when no read service is available.
    """


# DriverError > ServiceUnavailable > IncompleteCommit
class IncompleteCommit(ServiceUnavailable):
    """ Raised when the client looses connection while committing a transaction

    Raised when a disconnection occurs while still waiting for a commit
    response. For non-idempotent write transactions, this leaves the data
    in an unknown state with regard to whether the transaction completed
    successfully or not.
    """

    def is_retryable(self) -> bool:
        return False


# DriverError > ConfigurationError
class ConfigurationError(DriverError):
    """ Raised when there is an error concerning a configuration.
    """


# DriverError > ConfigurationError > AuthConfigurationError
class AuthConfigurationError(ConfigurationError):
    """ Raised when there is an error with the authentication configuration.
    """


# DriverError > ConfigurationError > CertificateConfigurationError
class CertificateConfigurationError(ConfigurationError):
    """ Raised when there is an error with the certificate configuration.
    """


class UnsupportedServerProduct(Exception):
    """ Raised when an unsupported server product is detected.
    """
