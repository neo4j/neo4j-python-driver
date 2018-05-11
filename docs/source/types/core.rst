===============
Core Data Types
===============

The core types supported by Cypher all map to core types in Python.
Booleans, Integers, Floats and Strings can all be stored as single value or array properties.
Byte Arrays can also be stored but do not have a single value equivalent.

=============  ========  ==============  ===========================  =============
Cypher Type    Property  Array Property  Python 2 Type                Python 3 Type
=============  ========  ==============  ===========================  =============
Null           *no*      *no*            :const:`None`                :const:`None`
Boolean        *yes*     *yes*           ``bool``                     ``bool``
Integer        *yes*     *yes*           ``int``/``long`` :sup:`[1]`  ``int``
Float          *yes*     *yes*           ``float``                    ``float``
String         *yes*     *yes*           ``unicode`` :sup:`[2]`       ``str``
Byte Array     *no*      *yes*           ``bytearray``                ``bytearray``
List           *no*      *no*            ``list``                     ``list``
Map            *no*      *no*            ``dict``                     ``dict``
=============  ========  ==============  ===========================  =============

.. admonition:: Notes

   1. While Cypher uses 64-bit signed integers, `int` can only hold integers up to `sys.maxint` in Python 2; `long` is used for values above this.
   2. In Python 2, a ``str`` passed as a parameter will always be implicitly converted to ``unicode`` via UTF-8.
