# Rust Extensions for a Faster Neo4j Bolt driver for Python

This project contains Rust extensions to speed up the [official Python driver for Neo4j](https://github.com/neo4j/neo4j-python-driver).

> **IMPORTANT**  
> This project is currently in **alpha** phase.


## Installation
Adjust your `requirements.txt` or similar the line `neo4j` with:
```
# remove:
# neo4j == 5.X.Y
# add:
neo4j-rust-ext ~= 5.X.Y.0
```

That's it!
You don't have to change your code.
This package will install the driver as its dependency and then inject itself in a place where the driver can find it and pick it up.


## Requirements
For many operating systems and architectures, the pre-built wheels will work out of the box.
If they don't, pip (or any other Python packaging front-ent) will try to build the extension from source.
Here's what you'll need for this:
 * Rust 1.65.0 or later:  
   https://www.rust-lang.org/tools/install
 * Header files for your Python installation.  
   For example, on Ubuntu, you need to install the `python3-dev` package.
