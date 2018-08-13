***********************
Sessions & Transactions
***********************

A :class:`.Transaction` is a unit of work that is either committed in its entirety or is rolled back on failure.
A :class:`.Session` is a logical container for one or more transactional units of work.
Sessions automatically provide guarantees of causal consistency within a clustered environment.

A session can be given a default `access mode` on construction.
This applies only in clustered environments and determines whether transactions carried out within that session should be routed to a `read` or `write` server.
Transaction functions within that session can override this access mode.

.. note::
    The driver does not parse Cypher statements and cannot determine whether a statement tagged as `read` or `write` is tagged correctly.
    Since the access mode is not passed to the server, this can allow a `write` statement to be executed in a `read` call on a single instance.
    Clustered environments are not susceptible to this loophole as cluster roles prevent it.

Neo4j supports three kinds of transaction: `auto-commit transactions`, `explicit transactions` and `transaction functions`.
Each has pros and cons but if in doubt, use a transaction function.

Auto-commit Transactions
========================
Auto-commit transactions are the simplest form, available via :meth:`.Session.run`.
These are fast and easy to use but support only one statement per transaction and are not automatically retried on failure.
Auto-commit transactions are also the only way to run ``USING PERIODIC COMMIT`` statements.

.. code-block:: python

    def create_person(driver, name):
        with driver.session() as session:
            return session.run("CREATE (a:Person {name:$name}) "
                               "RETURN id(a)", name=name).single().value()

Explicit Transactions
=====================
Explicit transactions support multiple statements and must be created with an explicit :meth:`.Session.begin_transaction` call.
Closing an explicit transaction can either happen automatically at the end of a ``with`` block, using the :attr:`.Transaction.success` attribute to determine success,
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
=====================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.
These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

.. code-block:: python

    def create_person(tx, name):
        return tx.run("CREATE (a:Person {name:$name}) "
                      "RETURN id(a)", name=name).single().value()

    with driver.session() as session:
        node_id = session.write_transaction(create_person, "Alice")


API
===
.. autoclass:: neo4j.Session
   :members:

.. autoclass:: neo4j.Transaction
   :members:
