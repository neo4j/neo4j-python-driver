.. _spatial-data-types:

*******************
Spatial Data Types
*******************

.. include:: _spatial_overview.rst


Point
=====

.. autoclass:: neo4j.spatial.Point
    :show-inheritance:
    :members: srid


CartesianPoint
==============

.. autoclass:: neo4j.spatial.CartesianPoint
    :show-inheritance:

    .. property:: srid
        :type: int

    .. property:: x
        :type: float

        Same value as ``point[0]``.

    .. property:: y
        :type: float

        Same value as ``point[1]``.

    .. property:: z
        :type: float

        Same value as ``point[2]``.

        Only available if the point is in space.


Examples
--------

.. code-block:: python

    point=CartesianPoint((1.23, 4.56)

    print(point.x, point.y)


.. code-block:: python

    point=CartesianPoint((1.23, 4.56, 7.89)

    print(point.x, point.y, point.z)


WGS84Point
==========

.. autoclass:: neo4j.spatial.WGS84Point
    :show-inheritance:

    .. property:: srid
        :type: int

    .. property:: x
        :type: float

        Same value as ``point[0]``.

    .. property:: y
        :type: float

        Same value as ``point[1]``.

    .. property:: z
        :type: float

        Same value as ``point[2]``.

        Only available if the point is in space.

    .. property:: longitude
        :type: float

        Alias for :attr:`.x`.

    .. property:: latitude
        :type: float

        Alias for :attr:`.y`.

    .. property:: height
        :type: float

        Alias for :attr:`.z`.

        Only available if the point is in space.


Examples
--------

.. code-block:: python

    point=WGS84Point((1.23, 4.56))
    print(point.longitude, point.latitude)


.. code-block:: python

    point=WGS84Point((1.23, 4.56, 7.89))
    print(point.longitude, point.latitude, point.height)
