===============
Core Data Types
===============

.. note::
   The Python types mentioned here are based on Python 3.
   Differences for Python 2 are highlighted in the `footnotes <#footnotes-for-python-2-users>`_.

Cypher supports a set of core data types that all map to built-in types in Python.
These include the common `Boolean`, `Integer`, `Float` and `String` types as well as `List` and `Map` that can hold heterogenous collections of any other type.
The core types with their general mappings are listed below:

================  =============
Cypher Type       Python Type
================  =============
Null              :const:`None`
Boolean           ``bool``
Integer           ``int``
Float             ``float``
String            ``str``
Bytes :sup:`[1]`  ``bytearray``
List              ``list``
Map               ``dict``
================  =============

.. admonition:: Notes

   1. `Bytes` is not an actual Cypher type but is transparently passed through when used in parameters or query results.


In reality, the actual conversions and coercions that occur as values are passed through the system are more complex than just a simple mapping.
The diagram below illustrates the actual mappings between the various layers, from driver to data store, for the core types.

.. image:: ../_images/core_type_mappings.svg
    :target: ../_images/core_type_mappings.svg


Footnotes for Python 2 users
============================

1. While Cypher uses 64-bit signed integers, ``int`` can only hold integers up to ``sys.maxint`` in Python 2; ``long`` is used for values above this.
2. Python 2 uses ``unicode`` instead of ``str`` for Unicode text.
   However, a Python 2 ``str`` passed as a parameter will always be implicitly converted to ``unicode`` via UTF-8.
