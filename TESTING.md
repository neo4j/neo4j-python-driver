# Neo4j Driver Testing

To run driver tests, [Tox](https://tox.readthedocs.io) is required as well as at least one version of Python.
The versions of Python supported by this driver are CPython 2.7, 3.4, 3.5 and 3.6.


## Unit Tests & Stub Tests

Unit tests and stub tests (those which make use of the [boltkit](https://github.com/neo4j-contrib/boltkit) stub server) can be run using:
```bash
$ tox
```

## Integration Tests

Each test run can also carry out integration tests against a specific version of Neo4j.
To enable integration tests, a server must be made available.
This can be either an existing server listening on the default Bolt port (7687) or a temporary installation from a particular package.
For example:
```bash
$ NEO4J_SERVER_PACKAGE=~/dist/neo4j-enterprise-3.1.1-unix.tar.gz tox
```

A web address can be provided as an alternative to a file path:
```bash
$ NEO4J_SERVER_PACKAGE=http://dist.neo4j.org/neo4j-enterprise-3.1.1-unix.tar.gz tox
```

If using an existing server, authentication details can be provided in a similar way:
```bash
$ NEO4J_USER=bob NEO4J_PASSWORD=secret tox
```


## Code Coverage

If [Coverage](https://coverage.readthedocs.io/) is installed, test runs automatically add data to a `.coverage` file.
To use this data, ensure that `coverage erase` is executed before commencing a test run;
a report can be viewed after the run with `coverage report --show-missing`.
