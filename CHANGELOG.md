# Neo4j Driver Change Log

## Version 4.0
- The package version has jumped from 1.7 directly to 4.0, in order to bring the version in line with Neo4j itself
- The package can now no longer be installed as `neo4j-driver`; use `pip install neo4j` instead
- Support has been dropped for Python 2.7; explicit support has been added for Pythons 3.7 and 3.8
- The `neo4j.v1` subpackage is now no longer available; all imports should be taken from the `neo4j` package instead
- Changed `session(access_mode)` from a positional to a keyword argument
- The `bolt+routing` scheme is now named `neo4j`
- Connections are now unencrypted by default; to reproduce former behaviour, add `encrypted=True` to Driver configuration
