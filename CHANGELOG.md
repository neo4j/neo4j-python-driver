# Neo4j Driver Change Log

## Version 2.0
- Package can now no longer be installed as `neo4j-driver`; use `pip install neo4j` instead
- Support dropped for Python 2.7; explicit support added for Python 3.7 and 3.8
- The `neo4j.v1` subpackage is now no longer available; all imports should be taken from the `neo4j` package instead
- Changed `session(access_mode)` from a positional to a keyword argument
