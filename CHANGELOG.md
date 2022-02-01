# Neo4j Driver Change Log (breaking/major changes only)

## Version 5.0

- Python 3.10 support added
- Python 3.6 support has been dropped.
- `Result`, `Session`, and `Transaction` can no longer be imported from
  `neo4j.work`. They should've been imported from `neo4j` all along.
- Experimental pipelines feature has been removed.
- Experimental async driver has been added.
- `ResultSummary.server.version_info` has been removed.  
  Use `ResultSummary.server.agent`, `ResultSummary.server.protocol_version`,
  or call the `dbms.components` procedure instead.
- SSL configuration options have been changed:
  - `trust` has been removed.  
    Use `trusted_certificates` instead which expects `None` or a `list`. See the
    API documentation for more details.
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
- Creation of a driver with `bolt[+s[sc]]://` scheme now raises an error if the
  URI contains a query part (a routing context). Previously, the routing context
  was silently ignored.
- `Result.single` now raises `ResultNotSingleError` if not exactly one result is
  available.
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


## Version 4.4

- Python 3.5 support has been dropped.


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
