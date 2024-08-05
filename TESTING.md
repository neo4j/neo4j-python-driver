# Neo4j Driver Testing
To run driver tests, [Tox](https://tox.readthedocs.io) is required as well as at least one version of Python.
The versions of Python supported by this driver are CPython 3.7 - 3.12

## Testing with TestKit
TestKit is the shared test suite used by all official (and some community contributed) Neo4j drivers to ensure consistent and correct behavior across all drivers.
When using TestKit to run tests, you don't have to take care of manually setting up or running unit tests or integration tests as shown below.
TestKit will take care of that for you and run many other tests as well.

TestKit can be found here: https://github.com/neo4j-drivers/testkit.
See its README for more information on how to use it.

## Unit Tests
Unit tests can be run using:
```bash
tox -f unit
```

## Integration Tests
Integration tests run against a real Neo4j server.
Hence, you must have a running server (either locally or remotely).
Make sure there's no data in any of the DBMS's databases and that an empty database `neo4j` is available and is set as the default database.

To allow the tests to connect to the server and choose the right tests to run, you must set the following environment variables:
 * `TEST_NEO4J_HOST`: host name or IP address of the server (e.g., `localhost`)
 * `TEST_NEO4J_PORT`: port number of the server (e.g., `7687`)
 * `TEST_NEO4J_USER`: username for logging into server (e.g., `neo4j`)
 * `TEST_NEO4J_PASS`: password for logging into server (e.g., `my-super-secret-p4$$w0rd`)
 * `TEST_NEO4J_SCHEME`: with wich URL scheme to connect to the server (e.g., `bolt`, `neo4j+ssc`)
 * `TEST_NEO4J_EDITION`: what edition the server is running (e.g., `enterprise`, `community`)
 * `TEST_NEO4J_VERSION`: what version the server is running (e.g., `4.4.36`, `5.22.0`)
 * `TEST_NEO4J_IS_CLUSTER`: whether the remote is a cluster or not (e.g., `true`/`1`, `false`/`0`)

You can then run the integration tests with:
```bash
tox -f integration
```
