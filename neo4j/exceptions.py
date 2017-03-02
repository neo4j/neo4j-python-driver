#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
This module contains the core driver exceptions.
"""


class AddressError(Exception):
    """ Raised when a network address is invalid.
    """


class ProtocolError(Exception):
    """ Raised when an unexpected or unsupported protocol event occurs.
    """


class ServiceUnavailable(Exception):
    """ Raised when no database service is available.
    """


class SecurityError(Exception):
    """ Raised when an action is denied due to security settings.
    """


class CypherError(Exception):
    """ Raised when the Cypher engine returns an error to the client.
    """

    message = None
    code = None
    classification = None
    category = None
    title = None
    metadata = None

    @classmethod
    def hydrate(cls, message=None, code=None, **metadata):
        message = message or "An unknown error occurred."
        code = code or "Neo.DatabaseError.General.UnknownError"
        try:
            _, classification, category, title = code.split(".")
        except ValueError:
            classification = "DatabaseError"
            category = "General"
            title = "UnknownError"
        if classification == "ClientError":
            try:
                error_class = client_errors[code]
            except KeyError:
                error_class = ClientError
        elif classification == "DatabaseError":
            error_class = DatabaseError
        elif classification == "TransientError":
            error_class = TransientError
        else:
            error_class = cls
        inst = error_class(message)
        inst.message = message
        inst.code = code
        inst.classification = classification
        inst.category = category
        inst.title = title
        inst.metadata = metadata
        return inst


class ClientError(CypherError):
    """ The Client sent a bad request - changing the request might yield a successful outcome.
    """


class DatabaseError(CypherError):
    """ The database failed to service the request.
    """


class TransientError(CypherError):
    """ The database cannot service the request right now, retrying later might yield a successful outcome.
    """


class ConstraintError(ClientError):
    """
    """


class CypherSyntaxError(ClientError):
    """
    """


class CypherTypeError(ClientError):
    """
    """


class Forbidden(ClientError, SecurityError):
    """
    """


class AuthError(ClientError, SecurityError):
    """ Raised when authentication failure occurs.
    """


client_errors = {

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
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase": Forbidden,
    "Neo.ClientError.General.ReadOnly": Forbidden,
    "Neo.ClientError.Schema.ForbiddenOnConstraintIndex": Forbidden,
    "Neo.ClientError.Schema.IndexBelongsToConstraint": Forbidden,
    "Neo.ClientError.Security.Forbidden": Forbidden,
    "Neo.ClientError.Transaction.ForbiddenDueToTransactionType": Forbidden,

    # AuthError
    "Neo.ClientError.Security.AuthorizationFailed": AuthError,
    "Neo.ClientError.Security.Unauthorized": AuthError,

}
