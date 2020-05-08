


class AutoTransaction():
    def __init__(self, connection, db, access_mode, bookmarks) :
        self._connection = connection
        self._result = None
        self._bookmarks = bookmarks
        self._access_mode = access_mode
        self._db = db
        self._bookmark = None

    def rollback(self):
        pass

    def run(self, query, parameters=None, **kwparameters):
        self._result = Result(self._connection, DataHydrator())
        self._result._run(query, parameters, self._db, self._access_mode, self._bookmarks, **kwparameters)
        return self._result

    def close(self):
        # If I have a pending result, detach it and forget about it
        if self._result:
            self._result.detach()
            self._result = None


class Transaction():
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the transaction is committed or rolled back on based on
    whether or not an exception is raised::

        with session.begin_transaction() as tx:
            pass

    """

    def __init__(self, connection) :
        self._connection = connection
        self._bookmark = None
        self._results = []
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exv_type, exc_value, traceback):
        if self._closed:
            return
        #if self._success is None:
        #    self._success = not bool(exc_type)
        success = not bool(exc_type)
        if success:
            self.commit()
        self._close()

    def _begin(self, db, bookmarks, access_mode, metadata, timeout, ):
        self._connection.begin(bookmarks=bookmarks, metadata=metadata, timeout=timeout, mode=access_mode, db=database)

    def run(self, query, parameters=None, **kwparameters):
        """ Run a Cypher query within the context of this transaction.

        The query is sent to the server lazily, when its result is
        consumed. To force the query to be sent to the server, use
        the :meth:`.Transaction.sync` method.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        queries below are all equivalent::

            >>> query = "CREATE (a:Person {name:{name}, age:{age}})"
            >>> tx.run(query, {"name": "Alice", "age": 33})
            >>> tx.run(query, {"name": "Alice"}, age=33)
            >>> tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: template Cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`neo4j.Result` object
        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        result = Result(self._connection, DataHydrator())
        result = result._run(query, parameters, None, None, None, **kwparameters)
        self.results.append(result)

    def sync(self):
        """ Force any queued queries to be sent to the server and
        all related results to be fetched and buffered.

        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        self._connection.send_all()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        metadata = {}
        try:
            self._connection.commit(on_success=metadata.update)
            self._connection.send_all()
            self._connection.fetch_all()
        except BoltIncompleteCommitError:
            raise ServiceUnavailable("Connection closed during commit")
        self._bookmark = metadata.get("bookmark")
        self._closed = True
        self._consume_results()
        return self._bookmark

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        metadata = {}
        self._connection.rollback(on_success=metadata.update)
        self._connection.send_all()
        self._connection.fetch_all()

    def _consume_results(self):
        for result in self._results:
            result.consume()
        self._results = []

    def _close(self):
        # TODO: Will always rollback..
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK.

        :raise TransactionError: if already closed
        """
        if self._closed:
            return
        self.rollback()
        self._consume_results()

    def closed(self):
        """ Indicator to show whether the transaction has been closed.
        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed

    def _assert_open(self):
        if self._closed:
            raise TransactionError("Transaction closed")


# Shared run func
# def _run(connection, query, parameters, db, access_mode, bookmarks, **kwparameters):
#     query_text = str(query)
#     query_metadata = getattr(query, "metadata", None)
#     query_timeout = getattr(query, "timeout", None)
#     parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))
# 
#     result_metadata = {
#         "query": query_text,
#         "parameters": parameters,
#         "server": server_info,
#         "protocol_version": protocol_version,
#     }
#     hydrant = DataHydrator()
#     result = Result(hydrant, result_metadata)
# 
#     run_metadata = {
#         "metadata": query_metadata,
#         "timeout": query_timeout,
#         "on_success": result_metadata.update,
#         "on_failure": fail,
#     }
#     if bookmarks:
#         run_metadata["bookmarks"] = bookmarks
# 
#     cx.run(
#         query_text,
#         parameters=parameters,
#         mode=access_mode,
#         bookmarks=bookmarks,
#         metadata=run_metadata["metadata"],
#         timeout=run_metadata["timeout"],
#         db=db,
#         on_success=run_metadata["on_success"],
#         on_failure=run_metadata["on_failure"],
#     )
# 
#     #TODO:
#     fetch_size = 10
#     self._connection.pull(
#         n=
#     self._connection.send_all()
#     self._connection.fetch_message()
# 
#     return result


