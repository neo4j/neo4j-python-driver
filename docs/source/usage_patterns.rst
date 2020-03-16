##############
Usage Patterns
##############

.. warning::
    This section is experimental! Breaking changes will occur!


******************
Simple Application
******************

.. code-block:: python

    from neo4j import GraphDatabase


    def print_friends_of(tx, name):
        query = "MATCH (a:Person)-[:KNOWS]->(f) WHERE a.name = {name} RETURN f.name"

        result = tx.run(query, name=name)

        for record in result:
            print(record["f.name"])


    if __name__ == "main":

        uri = "bolt://localhost:7687"
        driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

        with driver.session() as session:
            session.read_transaction(print_friends_of, "Alice")

        driver.close()


**********************************
Driver Initialization Work Pattern
**********************************

.. code-block:: python

    from neo4j import GraphDatabase, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
    from neo4j.exceptions import ServiceUnavailable

    uri = "bolt://localhost:7687"

    driver_config = {
        "encrypted": False,
        "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
        "user_agent": "example",
        "max_connection_lifetime": 1000,
        "max_connection_pool_size": 100,
        "keep_alive": False,
        "max_retry_time": 10,
        "resolver": None,
    }

    try:
        driver = GraphDatabase.driver(uri, auth=("neo4j", "password"), **driver_config)
        driver.close()
    except ServiceUnavailable as e:
        print(e)


Driver Initialization With Block Work Pattern
=============================================

.. Investigate the example 6 pattern for error handling
   https://www.python.org/dev/peps/pep-0343/#examples


.. code-block:: python

    from neo4j import GraphDatabase

    with GraphDatabase.driver(uri, auth=("neo4j", "password"), **driver_config) as driver:
        try:
            session = driver.session()
            session.close()
        except ServiceUnavailable as e:
            print(e)

***********************************
Session Initialization Work Pattern
***********************************

.. code-block:: python

    from neo4j import (
        ACCESS_READ,
        ACCESS_WRITE,
    )

    session_config = {
        "fetch_size": 100,
        "database": "default",
        "bookmarks": ["bookmark-1",],
        "access_mode": ACCESS_WRITE,
        "acquire_timeout": 60.0,
        "max_retry_time": 30.0,
        "initial_retry_delay": 1.0,
        "retry_delay_multiplier": 2.0,
        "retry_delay_jitter_factor": 0.2,
    }

    try:
        session = driver.session(access_mode=None, **session_config)
        session.close()
    except ServiceUnavailable as e:
        print(e)


Session Initialization With Block Work Pattern
==============================================

.. Investigate the example 6 pattern for error handling
   https://www.python.org/dev/peps/pep-0343/#examples


.. code-block:: python

    from neo4j.exceptions import ServiceUnavailable

    query = "RETURN 1 AS x"

    with driver.session(access_mode=None, **session_config) as session:
        try:
            result = session.run(query)
            for record in result:
                print(record["x"])
        except ServiceUnavailable as e:
            print(e)


*******************************
Session Autocommit Work Pattern
*******************************

.. code-block:: python

    statement = "RETURN $tag AS $name"

    kwparameters = {"name": "test", "tag": 123}

    session.run(query)

    session.run(query, parameters=None, **kwparameters)

    session.run(query, parameters={"name": "test", "tag": 123})

    session.run(query, parameters={"name": "test", "tag": 123}, **kwparameters)

    session.run(query, name="test", "tag"=123)


****************************************
Session Managed Transaction Work Pattern
****************************************

.. code-block:: python

    def test_work(tx, *args, **kwargs):
        query = "RETURN $tag AS $name"

        kwparameters = {"name": "test", "tag": 123}

        tx.run(query)

        tx.run(query, parameters=None, **kwparameters)

        tx.run(query, parameters={"name": "test", "tag": 123})

        tx.run(query, parameters={"name": "test", "tag": 123}, **kwparameters)

        tx.run(query, name="test", "tag"=123)


    session.read_transaction(test_work)

    session.read_transaction(test_work, *args, **kwargs)

    session.read_transaction(test_work, **kwargs)


    session.write_transaction(test_work)

    session.write_transaction(test_work, *args, **kwargs)

    session.write_transaction(test_work, **kwargs)


unit_of_work
============

.. code-block:: python

    from neo4j import unit_of_work


    @unit_of_work(timeout=10)
    def test_work(tx, *args, **kwargs):
        query = "RETURN $tag AS $name"

        result = tx.run(query)
        # The result needs to be consumed

    session.read_transaction(test_work)


.. code-block:: python

    from neo4j import unit_of_work


    @unit_of_work(metadata={"hello": 123})
    def test_work(tx, *args, **kwargs):
        query = "RETURN $tag AS $name"

        result = tx.run(query)
        # The result needs to be consumed

    session.read_transaction(test_work)


.. code-block:: python

    from neo4j import unit_of_work


    @unit_of_work(timeout=10, metadata={"hello": 123})
    def test_work(tx, *args, **kwargs):
        query = "RETURN $tag AS $name"

        result = tx.run(query)
        # The result needs to be consumed

    session.read_transaction(test_work)


The Query Object Work Pattern
=============================

.. code-block:: python

    from neo4j import Query


    def test_work(tx, *args, **kwargs):
        query = Query("RETURN 1 AS x, timeout=10, metadata={"hello": 123})

        result = tx.run(query)
        # The result needs to be consumed

    session.read_transaction(test_work)




*******************************
Transaction Object Work Pattern
*******************************

.. code-block:: python

    query = Query("RETURN 1 AS x, timeout=10, metadata={"hello": 123})

    tx = session.begin_transaction(bookmark=None, metadata=None, timeout=None)
    tx.run(query)
    tx.commit()
