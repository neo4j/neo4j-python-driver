#############################
Neo4j Python Driver |version|
#############################

The Official Neo4j Driver for Python.

Neo4j versions supported:

* Neo4j 4.2
* Neo4j 4.1
* Neo4j 4.0
* Neo4j 3.5

Python versions supported:

* Python 3.8
* Python 3.7
* Python 3.6
* Python 3.5


******
Topics
******

+ :ref:`api-documentation`

+ :ref:`temporal-data-types`

+ :ref:`breaking-changes`


.. toctree::
   :hidden:

   api.rst
   temporal_types.rst
   breaking_changes.rst


************
Installation
************

To install the latest stable release, use:

.. code:: bash

    python -m pip install neo4j


To install the latest pre-release, use:

.. code:: bash

    python -m pip install --pre neo4j


.. note::

   It is always recommended to install python packages for user space in a virtual environment.


Virtual Environment
===================

To create a virtual environment named sandbox, use:

.. code:: bash

    python -m venv sandbox

To activate the virtual environment named sandbox, use:

.. code:: bash

    source sandbox/bin/activate

To deactivate the current active virtual environment, use:

.. code:: bash

    deactivate


*************
Quick Example
*************

Creating nodes.

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def create_friend_of(tx, name, friend):
        tx.run("CREATE (a:Person)-[:KNOWS]->(f:Person {name: $friend}) "
               "WHERE a.name = $name "
               "RETURN f.name AS friend", name=name, friend=friend)

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Bob")

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Carl")

    driver.close()


Finding nodes.

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def get_friends_of(tx, name):
        friends = []
        result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = $name "
                             "RETURN f.name AS friend", name=name):
        for record in result:
            friends.append(record["friend"])
        return friends

    with driver.session() as session:
        friends = session.read_transaction(get_friends_of, "Alice")
        for friend in friends:
            print(friend)

    driver.close()


*******************
Example Application
*******************

.. code-block:: python

    import logging
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable

    class App:

        def __init__(self, uri, user, password):
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

        def close(self):
            # Don't forget to close the driver connection when you are finished with it
            self.driver.close()

        def create_friendship(self, person1_name, person2_name):
            with self.driver.session() as session:
                # Write transactions allow the driver to handle retries and transient errors
                result = session.write_transaction(
                    self._create_and_return_friendship, person1_name, person2_name)
                for record in result:
                    print("Created friendship between: {p1}, {p2}".format(
                        p1=record['p1'], p2=record['p2']))

        @staticmethod
        def _create_and_return_friendship(tx, person1_name, person2_name):

            # To learn more about the Cypher syntax,
            # see https://neo4j.com/docs/cypher-manual/current/

            # The Reference Card is also a good resource for keywords,
            # see https://neo4j.com/docs/cypher-refcard/current/

            query = (
                "CREATE (p1:Person { name: $person1_name }) "
                "CREATE (p2:Person { name: $person2_name }) "
                "CREATE (p1)-[:KNOWS]->(p2) "
                "RETURN p1, p2"
            )
            result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
            try:
                return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                        for record in result]
            # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

        def find_person(self, person_name):
            with self.driver.session() as session:
                result = session.read_transaction(self._find_and_return_person, person_name)
                for record in result:
                    print("Found person: {record}".format(record=record))

        @staticmethod
        def _find_and_return_person(tx, person_name):
            query = (
                "MATCH (p:Person) "
                "WHERE p.name = $person_name "
                "RETURN p.name AS name"
            )
            result = tx.run(query, person_name=person_name)
            return [record["name"] for record in result]

    if __name__ == "__main__":
        # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
        host_name = "example.com"
        port = 7687
        url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
        user = "<Username for Neo4j database>"
        password = "<Password for Neo4j database>"
        app = App(url, user, password)
        app.create_friendship("Alice", "David")
        app.find_person("Alice")
        app.close()


*****************
Other Information
*****************

* `Neo4j Documentation`_
* `The Neo4j Drivers Manual`_
* `Neo4j Quick Reference Card`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)
* `Migration Guide - Upgrade Neo4j drivers`_
* `Neo4j Aura`_

.. _`Python Driver 1.7`: https://neo4j.com/docs/api/python-driver/1.7/
.. _`Neo4j Documentation`: https://neo4j.com/docs/
.. _`The Neo4j Drivers Manual`: https://neo4j.com/docs/driver-manual/current/
.. _`Neo4j Quick Reference Card`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Migration Guide - Upgrade Neo4j drivers`: https://neo4j.com/docs/migration-guide/4.0/upgrade-driver/
.. _`Neo4j Aura`: https://neo4j.com/neo4j-aura/