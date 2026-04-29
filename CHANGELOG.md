# Neo4j Driver Change Log (breaking/major changes only)

See also https://github.com/neo4j/neo4j-python-driver/wiki for a full changelog.

## NEXT RELEASE
- No breaking or major changes.


## Version 6.2
- No breaking or major changes.


## Version 6.1
- Python 3.14 support added.


## Version 6.0
- Python 3.7, 3.8, and 3.9 support has been dropped.
- Remove deprecated package alias `neo4j-driver`. Use `pip install neo4j` instead.
- Remove `setup.py`. Please use a recent enough packaging/build tool that supports `pyproject.toml`
- Remove deprecated modules:
  - `neo4j.conf`
  - `neo4j.data`
  - `neo4j.meta`
  - `neo4j.packstream`
  - `neo4j.routing`
  - `neo4j.time.arithmetic`
  - `neo4j.time.clock_implementation`
  - `neo4j.time.hydration`
  - `neo4j.time.metaclasses`
  - `neo4j.work`
  - `neo4j.work.query`
  - `neo4j.work.summary`
- Remove deprecated exports from `neo4j`:
  - `log`, `Config`, `PoolConfig`, `SessionConfig`, `WorkspaceConfig` (internal - no replacement)
  - `SummaryNotificationPosition` (use `SummaryInputPosition` instead)
- `api.Version` has been removed as it's unused now.  
  `ServerInfo.protocol_version` now is a `tuple[int, int]` insteadof a `api.Version`.
  This should be drop-in replacement is most cases:
  - `Version` was a sup-type of `tuple[int, int]`
  - `ServerInfo.protocol_version` was already documented and typed as `tuple[int, int]`
  - `Version`'s additional methods were undocumented and shouldn't have been used
- Changed errors raised under certain circumstances
  - `ConfigurationError` if the passed `auth` parameters is not valid (instead of `AuthError`)
    - This improves the differentiation between `DriverError` for client-side errors and `Neo4jError` for server-side
      errors.
  - `access_mode` configuration option
    - `ValueError` on invalid value (instead of `ClientError`)
    - Consistently check the value (also for non-routing drivers)
  - `neo4j.exceptions.UnsupportedServerProduct` if no common bolt protocol version could be negotiated with the server
    (instead of internal `neo4j._exceptions.BoltHandshakeError`).  
    `UnsupportedServerProduct` is now a subclass of `ConfigurationError` (instead of `Exception` directly).
  - `connection_acquisition_timeout` configuration option
    - Raise `ValueError` on invalid values (instead of `ClientError`).
    - Consistently restrict the value to be strictly positive
    - New `ConnectionAcquisitionTimeoutError` (subclass of `DriverError`) instead of `ClientError`
      (subclass of `Neo4jError`) the timeout is exceeded.
      - This improves the differentiation between `DriverError` for client-side errors and `Neo4jError` for server-side
        errors.
    - The option now spans *anything* required to acquire a connection.  
      This includes potential fetching of routing tables which in itself requires acquiring a connection.
      Previously, the timeout would be restarted for such auxiliary connection acquisitions.
  - `TypeError` instead of `ValueError` when passing a `Query` object to `Transaction.run`.
  - `TransactionError` (subclass of `DriverError`) instead of `ClientError` (subclass of `Neo4jError`) when calling
    `session.run()` while an explicit transaction is active on that session.
    - This improves the differentiation between `DriverError` for client-side errors and `Neo4jError` for server-side
      errors.
    - It is now the same error raised as when trying to start an explicit transaction while another explicit transaction
      is already active.
- Slightly change `Neo4jError` and `ClientError`:
  - Properties `message` and `code` are always a `str` (instead of `str | None`).
  - Remove possibility to override/set `message` and `code` properties.
  - Remove undocumented, internal methods `Neo4jError.hydrate`, `Neo4jError.invalidates_all_connections`,
    and `Neo4jError.is_fatal_during_discovery`.
  - Remove deprecated method `Neo4jError.is_retriable`.  
    Use `Neo4jError.is_retryable` instead.
  - Change string representation of `Neo4jError` to include GQL error information.
- Remove deprecated `Record.__getslice__`. This magic method has been removed in Python 3.0.  
  If you were calling it directly, please use `Record.__getitem__(slice(...))` or simply `record[...]` instead.
- Bookmarks
  - Remove deprecated class `neo4j.Bookmark` in favor of `neo4j.Bookmarks`.
  - Remove deprecated method `session.last_bookmark()` in favor of `last_bookmarks()`.
  - Deprecate passing raw sting bookmarks as `initial_bookmarks` to `GraphDatabase.bookmark_manager()`.  
    Use a `neo4j.Bookmarks` object instead.
  - `Driver.session()` no longer accepts raw string bookmarks as `bookmarks` argument.  
    Use a `neo4j.Bookmarks` object instead.
- Remove deprecated `ServerInfo.connection_id`.  
  There is no replacement as this is considered internal information.
- Remove deprecated driver configuration option `trust`.  
  Use `trusted_certificates` instead.
  - Remove the associated constants `neo4j.TRUST_ALL_CERTIFICATES` and `neo4j.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES`.
- Remove deprecated `session.read_transaction` and `session.write_transaction`.  
  Instead, use `session.execute_read` and  `session.execute_write` respectively.
- Make undocumented classes `ResolvedAddress`, `ResolvedIPv4Address`, and `ResolvedIPv6Address` private.
- Rework `PreviewWarning`.
  - Remove `ExperimentalWarning` and turn the few left instances of it into `PreviewWarning`.
  - Deprecate importing `PreviewWarning` from `neo4j`.  
    Import it from `neo4j.warnings` instead.
- Make undocumented internal constants, helper functions, and other items private:
  - `neo4j.api`
    - `DRIVER_BOLT`
    - `DRIVER_NEO4J`
    - `SECURITY_TYPE_NOT_SECURE`
    - `SECURITY_TYPE_SECURE`
    - `SECURITY_TYPE_SELF_SIGNED_CERTIFICATE`
    - `check_access_mode`
    - `parse_neo4j_uri`
    - `parse_routing_context`
  - `neo4j.exceptions`
    - `CLASSIFICATION_CLIENT`
    - `CLASSIFICATION_DATABASE`
    - `CLASSIFICATION_TRANSIENT`
    - `ERROR_REWRITE_MAP`
    - `client_errors`
    - `transient_errors`
    - all other indirectly exposed items from imports (e.g. `typing` as `neo4j.exceptions.t`)
  - `neo4j.time`
    - `DATE_ISO_PATTERN`
    - `TIME_ISO_PATTERN`
    - `DURATION_ISO_PATTERN`
    - `NANO_SECONDS`
    - `AVERAGE_SECONDS_IN_MONTH`
    - `AVERAGE_SECONDS_IN_DAY`
    - `FORMAT_F_REPLACE`
    - `IS_LEAP_YEAR`
    - `DAYS_IN_YEAR`
    - `DAYS_IN_MONTH`
    - `round_half_to_even`
    - `symmetric_divmod`
    - `DateTimeType`
    - `DateType`
    - `TimeType`
    - all other indirectly exposed items from imports (e.g. `re` as `neo4j.time.re`)
  - `neo4j.spatial`
    - `hydrate_point`
    - `dehydrate_point`
    - `point_type`
  - `neo4j.GraphDatabase`
    - `.bolt_driver`
    - `.neo4j_driver`
  - `neo4j.BoltDriver` and `neo4j.Neo4jDriver`
    - `.open`
    - `.parse_target`
    - `.default_host`
    - `.default_port`
    - `.default_target`
  - `neo4j.graph`, `neo4j.addressing`, `neo4j.api`
    - indirectly exposed items from imports (e.g. `collections.abc.Mapping` as `neo4j.graph.Mapping`).
  - `BoltDriver` and `Neo4jDriver`
    - `.open`
    - `.parse_target`
    - `.default_host`
    - `.default_port`
    - `.default_target`
  - `neo4j.debug`
    - `ColourFormatter`
    - `TaskIdFilter`
    - all other indirectly exposed items from imports (e.g. `asyncio` as `neo4j.debug.asyncio`)
- Deprecate `ClockTime` and its accessors
  - For each `neo4j.time.Date`, `neo4j.time.DateTime`, `neo4j.time.Time`
    - `from_clock_time` and `to_clock_time` methods
  - `neo4j.time.ClockTime` itself
- Raise `ConfigurationError` instead of ignoring the routing context (URI query parameters) when creating a direct
  driver ("bolt[+s[sc]]://" scheme).
- Change behavior of closed drivers:
  - Raise `DriverError` on using the closed driver.
  - Calling `driver.close()` again is now a no-op.
- No longer implicitly closing drivers and sessions in `__del__()` (finalizer/destructor).  
  Make sure to call `.close()` on them explicitly or use them in a `with` statement.
- Make `Summary.summary_notifications` a `tuple` instead of a `list` and type it with `Sequence` to signify that it
  should be treated as immutable.
- Graph type sets (`neo4j.graph.EntitySetView`) can no longer by indexed by legacy `id` (`int`, e.g., `graph.nodes[0]`).  
  Use the `element_id` instead (`str`, e.g., `graph.nodes["..."]`).
- Make all comparator magic methods return `NotImplemented` instead of `False` (or raising `TypeError` in some
  instances) if the other operand is not of a supported type.
  This means that when comparing a driver type with another type is doesn't support, the other type get the chance to
  handle the comparison.  
  Affected types:
  - `neo4j.Record`
  - `neo4j.graph.Node`, `neo4j.graph.Relationship`, `neo4j.graph.Path`
  - `neo4j.time.Date`, `neo4j.time.Time`, `neo4j.time.DateTime`
  - `neo4j.spatial.Point` (and subclasses)
- Configuring the driver with a URL that cannot be DNS resolved will raise a (retryable) `ServiceUnavailable` error
  instead of a `ValueError`.
- Separate out log entries that are session-related (including transaction retries)
  form sub-logger `neo4j.pool` to a new sub-logger `neo4j.session`.
- Notifications:
  - Deprecate notifications and related APIs:
    - `ResultSummary.notifications`
    - `ResultSummary.summary_notifications`
    - `neo4j.SummaryNotification`
    - `neo4j.NotificationCategory`
    - `neo4j.NotificationDisabledCategory`
    - `notifications_disabled_categories` driver and session configuration option
      in favor of `notifications_disabled_classifications`
  - Stabilize GQL status objects (use this instead of notifications):
    - `ResultSummary.gql_status_objects`
    - `neo4j.GqlStatusObject`
    - (`neo4j.exceptions.GqlError`, `neo4j.exceptions.GqlErrorClassification`)
- On failed liveness check (s. `liveness_check_timeout` configuration option), the driver will no longer remove the
  remote from the cached routing tables, but only close the connection under test.  
  This aligns the driver with the other official Neo4j drivers.
- The driver incorrectly applied a timeout hint received from the server to both read and write I/O operations.
  It is now only applied to read I/O operations.  
  In turn, a new configuration option `connection_write_timeout` with a default value of `30 seconds` is introduced.
- Adjust string representation(s) of several types:
  - `SummaryCounters`: `repr` and `str` to conform with Python's recommendations.
  - `Point` and its subclasses: `repr` to conform with Python's recommendations.
  - `Duration` and `ClockTime`: `repr` to be more consistent with other temporal driver types.
  - `GqlStatusObject`: `repr` to conform with Python's recommendations.
  - `SummaryInputPosition`: `repr` to be more informative.


## Version 5.28
- Since the types of `Relationship`s are tied to the `Graph` object they belong to, fixing `pickle` support for graph types means that `Relationship`s with the same name will have a different type after `deepcopy`ing or pickling and unpickling them or their graph.
  For more details, see https://github.com/neo4j/neo4j-python-driver/pull/1133
- Drop undocumented support for Bolt protocol versions 4.1.


## Version 5.27
- No breaking or major changes.


## Version 5.26
- Python 3.13 support added.
- Deprecated setting attributes on `Neo4jError` like `message` and `code`.
- Deprecated undocumented method `Neo4jError.hydrate`.  
  It's internal and should not be used by client code.


## Version 5.25
- No breaking or major changes.


## Version 5.24
- Made `neo4j.auth_management.RotatingClientCertificateProvider` and
  `...AsyncRotatingClientCertificateProvider` (in preview)
  abstract classes, meaning they can no longer be instantiated directly.  
  Please use the provided factory methods instead:
  `neo4j.auth_management.RotatingClientCertificateProvider.rotating` and
  `....AsyncRotatingClientCertificateProvider.rotating()` respectively.


## Version 5.23
- No breaking or major changes.


## Version 5.22
- `SummaryNotificationPosition` has been deprecated in favor of `SummaryInputPosition`.


## Version 5.17 - 5.21
- No breaking or major changes.


## Version 5.16
- Implemented a hierarchical logger structure to improve log source
  identification and traceability.  
  Introduced child loggers:
  - `neo4j.io`: For socket and bolt protocol related logging.
  - `neo4j.pool`: For logs pertaining to connection pooling and routing.
  - `neo4j.auth_management`: For logging inside the provided AuthManager
    implementations.


## Version 5.15
- No breaking or major changes.


## Version 5.14
- Python 3.12 support added


## Version 5.13
- Deprecated using `neo4j.Driver` and `neo4j.AsyncDriver` after calling `.close()` on them,
  except for calling `.close()` again, which has no effects.


## Version 5.10 - 5.12
- No breaking or major changes.


## Version 5.9
- `neo4j.auth_management.ExpiringAuth`'s `expires_in` (in preview) was replaced
  by `expires_at`, which is a unix timestamp.  
  You can use `ExpiringAuth(some_auth).expires_in(123)` instead.


## Version 5.8
- Renamed experimental `neo4j.RoutingControl.READERS` to `READ` and `WRITERS` to
  `WRITE`.
- Renamed experimental `driver.query_bookmark_manager` to
  `execute_query_bookmark_manager`.
- Query argument to experimental `driver.execute_query` not is typed
  `LiteralString` instead of `str` to help mitigate accidental Cypher
  injections. There are rare use-cases where a computed string is necessary.
  Please use `# type: ignore`, or `typing.cast` to suppress the type checking in
  those cases.


## Version 5.7
- Deprecated importing from `neo4j.work` and its submodules.
  Everything should be imported directly from `neo4j` instead.


## Version 5.5 - 5.6
- No breaking or major changes.


## Version 5.4
- Undocumented helper methods `Neo4jError.is_fatal_during_discovery` and
  `Neo4jError.invalidates_all_connections` have been deprecated and will be
  removed without replacement in version 6.0.


## Version 5.3
- Python 3.11 support added
- Removed undocumented, unused `neo4j.data.map_type`
- Query strings are now typed `LiteralString` instead of `str` to help mitigate
  accidental Cypher injections. There are rare use-cases where a computed
  string is necessary. Please use `# type: ignore`, or `typing.cast` to
  suppress the type checking in those cases.
- The experimental bookmark manager feature was changed to no longer track
  bookmarks per database.  
  This effectively changes the signature of almost all bookmark
  manager related methods:
  - `neo4j.BookmarkManger` and `neo4j.AsyncBookmarkManger` abstract base
    classes:
    - `update_bookmarks` has no longer a `database` argument.
    - `get_bookmarks` has no longer a `database` argument.
    - The `get_all_bookmarks` method was removed.
    - The `forget` method was removed.
  - `neo4j.GraphDatabase.bookmark_manager` and
    `neo4j.AsyncGraphDatabase.bookmark_manager` factory methods:
    - `initial_bookmarks` is no longer a mapping from database name
      to bookmarks but plain bookmarks.
    - `bookmarks_supplier` no longer receives the database name as
      an argument.
    - `bookmarks_consumer` no longer receives the database name as
      an argument.  


## Version 5.1 - 5.2
- No breaking or major changes.


## Version 5.0
- Python 3.10 support added
- Python 3.6 support has been dropped.
- `Result`, `Session`, and `Transaction` can no longer be imported from
  `neo4j.work`. They should've been imported from `neo4j` all along.  
  Remark: It's recommended to import everything needed directly from `noe4j` if
  available, not its submodules or subpackages.
- Experimental pipelines feature has been removed.
- Async driver (i.e., support for asyncio) has been added.
- `ResultSummary.server.version_info` has been removed.  
  Use `ResultSummary.server.agent`, `ResultSummary.server.protocol_version`,
  or call the `dbms.components` procedure instead.
- SSL configuration options have been changed:
  - `trust` has been deprecated and will be removed in a future release.  
    Use `trusted_certificates` instead.
    See the API documentation for more details.
- `neo4j.time` module:
  - `Duration`
    - The constructor does not accept `subseconds` anymore.  
      Use `milliseconds`, `microseconds`, or `nanoseconds` instead.
    - The property `subseconds` has been removed.  
      Use `nanoseconds` instead.
    - The property `hours_minutes_seconds` has been removed.  
      Use `hours_minutes_seconds_nanoseconds` instead.
    - For all math operations holds: they are element-wise on
      (`months`, `days`, `nanoseconds`).
      This affects (i.e., changes) the working of `//`, `%`, `/`, and `*`.
      - Years are equal to 12 months.
      - Weeks are equal to 7 days.
      - `seconds`, `milliseconds`, `microseconds`, and `nanoseconds` are
        implicitly converted to `nanoseconds` or `seconds` as fit.
    - Multiplication and division allow for floats but will always result in
      integer values (round to nearest even).
  - `Time`
    - The constructor does not accept `float`s for `second` anymore.  
      Use `nanosecond` instead.
    - Ticks are now nanoseconds since midnight (`int`).
      - The property `ticks_ns` has been renamed to `ticks`.  
        The old `ticks` is no longer supported.
      - The property`from_ticks_ns` has been renamed to `from_ticks`.  
        The old `from_ticks` is no longer supported.
    - The property `second` returns an `int` instead of a `float`.  
      Use `nanosecond` to get the sub-second information.
    - The property `hour_minute_second` has been removed.  
      Use `hour_minute_second_nanosecond` instead.
  - `DateTime`
    - The property `hour_minute_second` has been removed.  
      Use `hour_minute_second_nanosecond` instead.
    - The property `second` returns an `int` instead of a `float`.  
      Use `nanosecond` to get the sub-second information.
- Creation of a driver with `bolt[+s[sc]]://` scheme and a routing context has
  been deprecated and will raise an error in the Future. The routing context was
  and will be silently ignored until then.
- Bookmarks
  - `Session.last_bookmark` was deprecated. Its behaviour is partially incorrect
    and cannot be fixed without breaking its signature.  
    Use `Session.last_bookmarks` instead.
  - `neo4j.Bookmark` was deprecated.  
    Use `neo4j.Bookmarks` instead.
- Deprecated closing of driver and session objects in their destructor.
  This behaviour is non-deterministic as there is no guarantee that the
  destructor will ever be called. A `ResourceWarning` is emitted instead.  
  Make sure to configure Python to output those warnings when developing your
  application locally (it does not by default).
- Result scope:  
  - Records of Results cannot be accessed (`peek`, `single`, `iter`, ...)
    after their owning transaction has been closed, committed, or rolled back.
    Previously, this would yield undefined behavior.
    It now raises a `ResultConsumedError`.
  - Records of Results cannot be accessed (`peek`, `single`, `iter`, ...)
    after the Result has been consumed (`Result.consume()`).
    Previously, this would always yield no records.
    It now raises a `ResultConsumedError`.
  - New method `Result.closed()` can be used to check for this condition if
    necessary.
- The undocumented return value of `driver.verify_connectivity()` has been
  removed. If you need information about the remote server, use
  `driver.get_server_info()` instead.
- Transaction functions (a.k.a. managed transactions):  
  The first argument of transaction functions is now a `ManagedTransaction`
  object. It behaves exactly like a regular `Transaction` object, except it
  does not offer the `commit`, `rollback`, `close`, and `closed` methods.  
  Those methods would have caused a hard to interpreted error previously. Hence,
  they have been removed.
- Deprecated Nodes' and Relationships' `id` property (`int`) in favor of
  `element_id` (`str`).  
  This also affects `Graph` objects as indexing `graph.nodes[...]` and
  `graph.relationships[...]` with integers has been deprecated in favor of
  indexing them with strings.
- `ServerInfo.connection_id` has been deprecated and will be removed in a
  future release. There is no replacement as this is considered internal
  information.
- Output of logging helper `neo4j.debug.watch` changes
  - ANSI colour codes for log output are now opt-in
  - Prepend log format with log-level (if colours are disabled)
  - Prepend log format with thread name and id
- Deprecated `neo4j.exceptions.Neo4jError.is_retriable()`.  
  Use `neo4j.exceptions.Neo4jError.is_retryable()` instead.
- Importing submodules from `neo4j.time` (`neo4j.time.xyz`) has been deprecated.
  Everything needed should be imported from `neo4j.time` directly.
- `neo4j.spatial.hydrate_point` and `neo4j.spatial.dehydrate_point` have been
  deprecated without replacement. They are internal functions.
- Importing `neo4j.packstream` has been deprecated. It's internal and should not
  be used by client code.
- Importing `neo4j.routing` has been deprecated. It's internal and should not
  be used by client code.
- Importing `neo4j.config` has been deprecated. It's internal and should not
  be used by client code.
- `neoj4.Config`, `neoj4.PoolConfig`, `neoj4.SessionConfig`, and
  `neoj4.WorkspaceConfig` have been deprecated without replacement. They are
  internal classes.
- Importing `neo4j.meta` has been deprecated. It's internal and should not
  be used by client code. `ExperimantalWarning` should be imported directly from
  `neo4j`. `neo4j.meta.version` is exposed through `neo4j.__vesrion__`
- Importing `neo4j.data` has been deprecated. It's internal and should not
  be used by client code. `Record` should be imported directly from `neo4j`
  instead. `neo4j.data.DataHydrator` and `neo4j.data.DataDeydrator` have been
  removed without replacement.
- Removed undocumented config options that had no effect:
  `protocol_version` and `init_size`.
- Introduced `neo4j.exceptions.SessionError` that is raised when trying to
  execute work on a closed or otherwise terminated session.
- Removed deprecated config options `update_routing_table_timeout` and
  `session_connection_timeout`.  
  Server-side keep-alives communicated through configuration hints together with
  the config option `connection_acquisition_timeout` are sufficient to avoid the
  driver getting stuck.
- Deprecate `Session.read_transaction` and `Session.write_transaction` in favor
  of `Session.execute_read` and `Session.execute_write` respectively.


## Version 4.4
- Python 3.5 support has been dropped.


## Version 4.3
- Python 3.9 support added


## Version 4.2
- No breaking or major changes.


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
