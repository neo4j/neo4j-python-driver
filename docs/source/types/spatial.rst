.. _spatial-data-types:

*******************
Spatial Data Types
*******************

.. include:: _spatial_overview.rst


Point
=====

.. autoclass:: neo4j.spatial.Point
    :show-inheritance:
    :members:


CartesianPoint
==============

.. autoclass:: neo4j.spatial.CartesianPoint
    :show-inheritance:

    .. property:: x
        :type: float

        Same value as ``point[0]``.

    .. property:: y
        :type: float

        Same value as ``point[1]``.

    .. property:: z
        :type: float

        Same value as ``point[2]``.

        Only available if the point is in 3D space.


Examples
--------

.. code-block:: python

    from neo4j.spatial import CartesianPoint

    point = CartesianPoint((1.23, 4.56)
    print(point.x, point.y, point.srid)
    # 1.23 4.56 7203


.. code-block:: python

    from neo4j.spatial import CartesianPoint

    point = CartesianPoint((1.23, 4.56, 7.89)
    print(point.x, point.y, point.z, point.srid)
    # 1.23 4.56 7.8 9157


WGS84Point
==========

.. autoclass:: neo4j.spatial.WGS84Point
    :show-inheritance:

    .. property:: x
        :type: float

        Same value as ``point[0]``.

    .. property:: y
        :type: float

        Same value as ``point[1]``.

    .. property:: z
        :type: float

        Same value as ``point[2]``.

        Only available if the point is in 3D space.

    .. property:: longitude
        :type: float

        Alias for :attr:`.x`.

    .. property:: latitude
        :type: float

        Alias for :attr:`.y`.

    .. property:: height
        :type: float

        Alias for :attr:`.z`.

        Only available if the point is in 3D space.


Examples
--------

.. code-block:: python

    from neo4j.spatial import WGS84Point

    point = WGS84Point((1.23, 4.56))
    print(point.longitude, point.latitude, point.srid)
    # 1.23 4.56 4326


.. code-block:: python

    from neo4j.spatial import WGS84Point

    point = WGS84Point((1.23, 4.56, 7.89))
    print(point.longitude, point.latitude, point.height, point.srid)
    # 1.23 4.56 7.89 4979
