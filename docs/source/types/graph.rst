================
Graph Data Types
================

Cypher queries can return entire graph structures as well as individual property values.

The graph data types detailed here model graph data returned from a Cypher query.
Graph values cannot be passed in as parameters as it would be unclear whether the entity was intended to be passed by reference or by value.
The identity or properties of that entity should be passed explicitly instead.

The driver contains a corresponding class for each of the graph types that can be returned.

=============  ======================
Cypher Type    Python Type
=============  ======================
Node           :class:`.Node`
Relationship   :class:`.Relationship`
Path           :class:`.Path`
=============  ======================

.. class:: neo4j.types.graph.Graph

    A local, self-contained graph object that acts as a container for :class:`.Node` and :class:`.Relationship` instances.
    This is typically obtained via the :meth:`.BoltStatementResult.graph` method.

    .. autoattribute:: nodes

    .. autoattribute:: relationships

    .. automethod:: relationship_type


.. class:: neo4j.types.graph.Node

    .. describe:: node == other

        Compares nodes for equality.

    .. describe:: node != other

        Compares nodes for inequality.

    .. describe:: hash(node)

        Computes the hash of a node.

    .. describe:: len(node)

        Returns the number of properties on a node.

    .. describe:: iter(node)

        Iterates through all properties on a node.

    .. describe:: node[key]

        Returns a node property by key.
        Raises :exc:`KeyError` if the key does not exist.

    .. describe:: key in node

        Checks whether a property key exists for a given node.

    .. autoattribute:: graph

    .. autoattribute:: id

    .. autoattribute:: labels

    .. automethod:: get

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items


.. class:: neo4j.types.graph.Relationship

    .. describe:: relationship == other

        Compares relationships for equality.

    .. describe:: relationship != other

        Compares relationships for inequality.

    .. describe:: hash(relationship)

        Computes the hash of a relationship.

    .. describe:: len(relationship)

        Returns the number of properties on a relationship.

    .. describe:: iter(relationship)

        Iterates through all properties on a relationship.

    .. describe:: relationship[key]

        Returns a relationship property by key.
        Raises :exc:`KeyError` if the key does not exist.

    .. describe:: key in relationship

        Checks whether a property key exists for a given relationship.

    .. describe:: type(relationship)

        Returns the type (class) of a relationship.
        Relationship objects belong to a custom subtype based on the type name in the underlying database.

    .. autoattribute:: graph

    .. autoattribute:: id

    .. autoattribute:: nodes

    .. autoattribute:: start_node

    .. autoattribute:: end_node

    .. autoattribute:: type

    .. automethod:: get

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items


.. class:: neo4j.types.graph.Path

    .. describe:: path == other

        Compares paths for equality.

    .. describe:: path != other

        Compares paths for inequality.

    .. describe:: hash(path)

        Computes the hash of a path.

    .. describe:: len(path)

        Returns the number of relationships in a path.

    .. describe:: iter(path)

        Iterates through all the relationships in a path.

    .. autoattribute:: graph

    .. autoattribute:: nodes

    .. autoattribute:: start_node

    .. autoattribute:: end_node

    .. autoattribute:: relationships
