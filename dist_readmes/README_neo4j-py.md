# Pure Python Neo4j Bolt Driver
This project contains a pure Python version of the [official Python driver for Neo4j](https://github.com/neo4j/neo4j-python-driver).

You're very unlikely to actually need this package unless you're experiencing issues with the Rust extensions.


## Installation
Adjust your dependencies (`requirements.txt`, `pyproject.toml` or similar) like so:
```
# remove:
# neo4j == X.Y.Z  # Needs to be at least 5.23.0 for a matching neo4j-py version to exist.
#                   Before that, neo4j was pure-python by default anyway.
# add:
neo4j-py == X.Y.Z
```

I.e., install the same version of `neo4j-py` as you would install of `neo4j`.  
That's it!
You don't have to change your code but can use the driver as you normally would.

For more information, see the README of [the driver](https://github.com/neo4j/neo4j-python-driver).
