> **Warning**
> This package is deprecated and will stop receiving updates starting with version 6.0.0.
> Please install ``neo4j`` instead (which is an alias, i.e., a drop-in replacement).
> See https://pypi.org/project/neo4j/ .


# Neo4j Bolt Driver for Python
This repository contains the official Neo4j driver for Python.

Starting with 5.0, the Neo4j Drivers will be moving to a monthly release cadence.
A minor version will be released on the last Friday of each month so as to maintain versioning consistency with the core product (Neo4j DBMS) which has also moved to a monthly cadence.

As a policy, patch versions will not be released except on rare occasions.
Bug fixes and updates will go into the latest minor version and users should upgrade to that.
Driver upgrades within a major version will never contain breaking API changes.

See also: https://neo4j.com/developer/kb/neo4j-supported-versions/

 * Python 3.12 supported.
 * Python 3.11 supported.
 * Python 3.10 supported.
 * Python 3.9 supported.
 * Python 3.8 supported.
 * Python 3.7 supported.


## Installation
To install the latest stable version, use:

```bash
pip install neo4j
```

[//]: # (TODO: 7.0 - remove this note)

> **Note**  
> ``neo4j-driver`` is the old name for this package.
> It is now deprecated and will receive no further updates starting with 6.0.0.
> Make sure to install ``neo4j`` as shown above.


## Rust Extensions for Better Performance
The driver comes with optional Rust extensions that can significantly improve performance.

For many operating systems and architectures, the pre-built wheels will work out of the box, and pip will be able to download pre-compiled binaries.
If they don't, pip (or any other Python packaging front-end) will try to build the extensions from source.
Here's what you'll need for this:
* Rust 1.65.0 or later:  
  https://www.rust-lang.org/tools/install
* Further build tools (depending on the platform).  
  E.g., `gcc` on Ubuntu: `sudo apt install gcc`

If compilation fails, the driver will fall back to the pure Python implementation.
If you'd rather not have the Rust extensions at all, you can install the pure Python version of the driver via
[neo4j-py](https://pypi.org/project/neo4j-py/).
Especially for debugging purposes, this can prove useful.
If, however, you'd rather want the installation process to fail if the Rust extensions can't be installed/built, you can install [neo4j-rust-ext](https://pypi.org/project/neo4j-rust-ext/) instead.


## Quick Example
```python
from neo4j import GraphDatabase, RoutingControl


URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "password")


def add_friend(driver, name, friend_name):
    driver.execute_query(
        "MERGE (a:Person {name: $name}) "
        "MERGE (friend:Person {name: $friend_name}) "
        "MERGE (a)-[:KNOWS]->(friend)",
        name=name, friend_name=friend_name, database_="neo4j",
    )


def print_friends(driver, name):
    records, _, _ = driver.execute_query(
        "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
        "RETURN friend.name ORDER BY friend.name",
        name=name, database_="neo4j", routing_=RoutingControl.READ,
    )
    for record in records:
        print(record["friend.name"])


with GraphDatabase.driver(URI, auth=AUTH) as driver:
    add_friend(driver, "Arthur", "Guinevere")
    add_friend(driver, "Arthur", "Lancelot")
    add_friend(driver, "Arthur", "Merlin")
    print_friends(driver, "Arthur")
```


## Further Information
* [The Neo4j Operations Manual][ops-manual] (docs on how to run a Neo4j server)
* [The Neo4j Python Driver Manual][driver-manual] (good introduction to this driver)
* [Python Driver API Documentation][api-docs] (full API documentation for this driver)
* [Neo4j Cypher Cheat Sheet][cypher-cheat-sheet] (summary of Cypher syntax - Neo4j's graph query language)
* [Example Project][example] (small web application using this driver)
* [GraphAcademy][graph-academy] (interactive, free online trainings for Neo4j)
* [Driver Wiki][wiki] (includes change logs)
* [Neo4j Migration Guide][migration-guide]

[ops-manual]: https://neo4j.com/docs/operations-manual/current/
[driver-manual]: https://neo4j.com/docs/python-manual/current/
[api-docs]: https://neo4j.com/docs/api/python-driver/current/
[cypher-cheat-sheet]: https://neo4j.com/docs/cypher-cheat-sheet/
[example]: https://github.com/neo4j-examples/movies-python-bolt
[graph-academy]: https://graphacademy.neo4j.com/categories/python/
[wiki]: https://github.com/neo4j/neo4j-python-driver/wiki
[migration-guide]: https://neo4j.com/docs/migration-guide/current/
