# Rust Extensions for a Faster Neo4j Bolt Driver for Python
This project contains Rust extensions to speed up the [official Python driver for Neo4j](https://github.com/neo4j/neo4j-python-driver).

The exact performance depends on the use-case but has been measured to be up to 10x faster but never slower.
Use-cases moving only few but big records out of the DBMS tend to benefit the most.


## Installation
Adjust your dependencies (`requirements.txt`, `pyproject.toml` or similar) like so:
```
# remove:
# neo4j == X.Y.Z  # Needs to be at least 5.14.1 for matching Rust extensions to exist.
# add:
neo4j-rust-ext == X.Y.Z
```

I.e., install the same version of `neo4j-rust-ext` as you would install of `neo4j`.  
That's it!
You don't have to change your code but can use the driver as you normally would.

For more information, see the README of [the driver](https://github.com/neo4j/neo4j-python-driver).


## Requirements
For many operating systems and architectures, the pre-built wheels will work out of the box.
If they don't, pip (or any other Python packaging front-end) will try to build the extensions from source.
Here's what you'll need for this:
 * Rust 1.65.0 or later:  
   https://www.rust-lang.org/tools/install
 * Further build tools (depending on the platform).  
   E.g., `gcc` on Ubuntu: `sudo apt install gcc`
