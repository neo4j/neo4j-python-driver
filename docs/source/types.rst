******************
Cypher Type System
******************

The Cypher Type System comprises a general purpose set of data types along with some more specialist types.
This type system applies to all values passed in as parameters as well as all those received within results, with a few caveats.
Note that only a subset of types may be used as :class:`.Node` or :class:`.Relationship` properties.


Core Types
==========

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


Graph Types
===========

The graph types model graph data returned from a Cypher query.
Graph values cannot be passed in as parameters as it would be unclear whether the entity was intended to be passed by reference or by value.
The identity or properties of that entity should be passed explicitly instead.

All graph values returned within a given :class:`.StatementResult` are contained within a :class:`.Graph` instance, accessible via :meth:`.StatementResult.graph`.
The driver contains a corresponding class for each of the graph types that can be returned.

=============  ========  ==============  ======================
Cypher Type    Property  Array Property  Python Type
=============  ========  ==============  ======================
Node           *no*      *no*            :class:`.Node`
Relationship   *no*      *no*            :class:`.Relationship`
Path           *no*      *no*            :class:`.Path`
=============  ========  ==============  ======================

.. autoclass:: neo4j.v1.types.graph.Graph
   :members:

.. autoclass:: neo4j.v1.types.graph.Entity
   :members:

.. autoclass:: neo4j.v1.types.graph.EntitySetView
   :members:

.. autoclass:: neo4j.v1.types.graph.Node
   :members:
   :inherited-members:

.. autoclass:: neo4j.v1.types.graph.Relationship
   :members:
   :inherited-members:

.. autoclass:: neo4j.v1.types.graph.Path
   :members:


Spatial Types
=============

=============  ========  ==============  ======================
Cypher Type    Property  Array Property  Python Type
=============  ========  ==============  ======================
Point          *yes*     *yes*           :class:`.Point`
=============  ========  ==============  ======================

.. autoclass:: neo4j.v1.types.spatial.Point
   :members:

.. autoclass:: neo4j.v1.types.spatial.CartesianPoint
   :members:
   :inherited-members:

.. autoclass:: neo4j.v1.types.spatial.WGS84Point
   :members:
   :inherited-members:


Temporal Types
==============

=============  ========  ==============  ======================================
Cypher         Property  Array Property  Python
=============  ========  ==============  ======================================
Date           *yes*     *yes*           ``datetime.date``
Time           *yes*     *yes*           ``datetime.time`` (tzinfo != None)
LocalTime      *yes*     *yes*           ``datetime.time`` (tzinfo == None)
DateTime       *yes*     *yes*           ``datetime.datetime`` (tzinfo != None)
LocalDateTime  *yes*     *yes*           ``datetime.datetime`` (tzinfo == None)
Duration       *yes*     *yes*           :class:`.duration` :sup:`[1]`
=============  ========  ==============  ======================================

.. admonition:: Notes

   1. A ``datetime.timespan`` value passed as a parameter will always be implicitly converted to a :class:`.duration` value.

.. autoclass:: neo4j.v1.types.temporal.duration
   :members:
