**************
Usage Patterns
**************

Simple

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def print_friends_of(tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = {name} "
                             "RETURN f.name", name=name):
            print(record["f.name"])

    with driver.session() as session:
        session.read_transaction(print_friends_of, "Alice")