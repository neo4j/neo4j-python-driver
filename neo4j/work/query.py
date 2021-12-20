# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


class Query:
    """ Create a new query.

    :param text: The query text.
    :type text: str
    :param metadata: metadata attached to the query.
    :type metadata: dict
    :param timeout: seconds.
    :type timeout: int
    """
    def __init__(self, text, metadata=None, timeout=None):
        self.text = text

        self.metadata = metadata
        self.timeout = timeout

    def __str__(self):
        return str(self.text)


def unit_of_work(metadata=None, timeout=None):
    """This function is a decorator for transaction functions that allows extra control over how the transaction is carried out.

    For example, a timeout may be applied::

        @unit_of_work(timeout=100)
        def count_people_tx(tx):
            result = tx.run("MATCH (a:Person) RETURN count(a) AS persons")
            record = result.single()
            return record["persons"]

    :param metadata:
        a dictionary with metadata.
        Specified metadata will be attached to the executing transaction and visible in the output of ``dbms.listQueries`` and ``dbms.listTransactions`` procedures.
        It will also get logged to the ``query.log``.
        This functionality makes it easier to tag transactions and is equivalent to ``dbms.setTXMetaData`` procedure, see https://neo4j.com/docs/operations-manual/current/reference/procedures/ for procedure reference.
    :type metadata: dict

    :param timeout:
        the transaction timeout in seconds.
        Transactions that execute longer than the configured timeout will be terminated by the database.
        This functionality allows to limit query/transaction execution time.
        Specified timeout overrides the default timeout configured in the database using ``dbms.transaction.timeout`` setting.
        Value should not represent a duration of zero or negative duration.
    :type timeout: int
    """

    def wrapper(f):

        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        wrapped.metadata = metadata
        wrapped.timeout = timeout
        return wrapped

    return wrapper
