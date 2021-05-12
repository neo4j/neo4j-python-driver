
# Neo4j Driver Change Log


## Version 4.3

- Python 3.9 support added


## Version 4.2

- No driver changes have been made for Neo4j 4.2


## Version 4.1

- Routing context is now forwarded to the server for when required by server-side routing


## Version 4.0 - Breaking Changes

- The package version has jumped from `1.7` directly to `4.0`, in order to bring the version in line with Neo4j itself.
- The package can now no longer be installed as `neo4j-driver`; use `pip install neo4j` instead.
- The `neo4j.v1` subpackage is now no longer available; all imports should be taken from the `neo4j` package instead.
- Changed `session(access_mode)` from a positional to a keyword argument
- The `bolt+routing` scheme is now named `neo4j`
- Connections are now unencrypted by default; to reproduce former behaviour, add `encrypted=True` to Driver configuration
- Removed `transaction.success` flag usage pattern.

+ Python 3.8 supported.
+ Python 3.7 supported.
+ Python 3.6 supported.
+ Python 3.5 supported.
+ Python 3.4 support has been dropped.
+ Python 3.3 support has been dropped.
+ Python 3.2 support has been dropped.
+ Python 3.1 support has been dropped.
+ Python 3.0 support has been dropped.
+ Python 2.7 support has been dropped.
