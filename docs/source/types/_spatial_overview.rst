Cypher has built-in support for handling spatial values (points),
and the underlying database supports storing these point values as properties on nodes and relationships.

https://neo4j.com/docs/cypher-manual/current/syntax/spatial/


=================  =====================================
Cypher Type        Python Type
=================  =====================================
Point              :class:`neo4j.spatial.Point`

Point (Cartesian)  :class:`neo4j.spatial.CartesianPoint`
Point (WGS-84)     :class:`neo4j.spatial.WGS84Point`
=================  =====================================
