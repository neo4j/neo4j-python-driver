***********************
Sessions & Transactions
***********************

All database activity is co-ordinated through two mechanisms: the :class:`.Session` and the :class:`.Transaction`.
A :class:`.Transaction` is a unit of work that is either committed in its entirety or is rolled back on failure.
A :class:`.Session` is a logical container for any number of causally-related transactional units of work.
Sessions automatically provide guarantees of causal consistency within a clustered environment but multiple sessions can also be causally chained if required.


Sessions
========

Sessions provide the top-level of containment for database activity.
Session creation is a lightweight operation and sessions are `not` thread safe.

Connections are drawn from the :class:`neo4j.Driver` connection pool as required; an idle session will not hold onto a connection.

Sessions will often be created and destroyed using a `with` block context.
For example::

    with driver.session() as session:
        result = session.run("MATCH (a:Person) RETURN a.name")
        # do something with the result...

To construct a :class:`.Session` use the :meth:`.Driver.session` method.

.. class:: .Session

    .. automethod:: close

    .. automethod:: closed

    .. automethod:: run

    .. automethod:: sync

    .. automethod:: detach

    .. automethod:: next_bookmarks

    .. automethod:: last_bookmark

    .. automethod:: has_transaction

    .. automethod:: begin_transaction

    .. automethod:: read_transaction

    .. automethod:: write_transaction


Transactions
============

Neo4j supports three kinds of transaction: `auto-commit transactions`, `explicit transactions` and `transaction functions`.
Each has pros and cons but if in doubt, use a transaction function.

Auto-commit Transactions
------------------------
Auto-commit transactions are the simplest form of transaction, available via :meth:`.Session.run`.
These are easy to use but support only one statement per transaction and are not automatically retried on failure.
Auto-commit transactions are also the only way to run ``PERIODIC COMMIT`` statements, since this Cypher clause manages its own transactions internally.

.. code-block:: python

    def create_person(driver, name):
        with driver.session() as session:
            return session.run("CREATE (a:Person { name: $name }) "
                               "RETURN id(a)", name=name).single().value()

Explicit Transactions
---------------------
Explicit transactions support multiple statements and must be created with an explicit :meth:`.Session.begin_transaction` call.
This creates a new :class:`.Transaction` object that can be used to run Cypher.
It also gives applications the ability to directly control `commit` and `rollback` activity.

.. class:: .Transaction

    .. automethod:: run

    .. automethod:: sync

    .. automethod:: closed

    .. automethod:: commit

    .. automethod:: rollback

Closing an explicit transaction can either happen automatically at the end of a ``with`` block,
or can be explicitly controlled through the :meth:`.Transaction.commit` and :meth:`.Transaction.rollback` methods.
Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction.

.. code-block:: python

    def create_person(driver, name):
        with driver.session() as session:
            tx = session.begin_transaction()
            node_id = create_person_node(tx)
            set_person_name(tx, node_id, name)
            tx.commit()

    def create_person_node(tx):
        return tx.run("CREATE (a:Person)"
                      "RETURN id(a)", name=name).single().value()

    def set_person_name(tx, node_id, name):
        tx.run("MATCH (a:Person) WHERE id(a) = $id "
               "SET a.name = $name", id=node_id, name=name)

Transaction Functions
---------------------
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.
These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

.. code-block:: python

    def create_person(tx, name):
        return tx.run("CREATE (a:Person { name: $name }) "
                      "RETURN id(a)", name=name).single().value()

    with driver.session() as session:
        node_id = session.write_transaction(create_person, "Alice")

To exert more control over how a transaction function is carried out, the :func:`.unit_of_work` decorator can be used.

.. autofunction:: neo4j.work.simple.unit_of_work


Access modes
============

A session can be given a default `access mode` on construction.
This applies only in clustered environments and determines whether transactions carried out within that session should be routed to a `read` or `write` server by default.

Note that this mode is simply a default and not a constraint.
This means that transaction functions within a session can override the access mode passed to that session on construction.

.. note::
    The driver does not parse Cypher queries and cannot determine whether the access mode should be :code:`ACCESS_READ` or :code:`ACCESS_WRITE`.
    Since the access mode is not passed to the server, this can allow a :code:`ACCESS_WRITE` statement to be executed for a :code:`ACCESS_READ` call on a single instance.
    Clustered environments are not susceptible to this loophole as cluster roles prevent it.
    This behaviour should not be relied upon as the loophole may be closed in a future release.
