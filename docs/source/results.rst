*****************
Consuming Results
*****************

Every time a query is executed, a :class:`.Result` is returned.

This provides a handle to the result of the query, giving access to the records within it as well as the result metadata.

Each result consists of header metadata, zero or more :class:`.Record` objects and footer metadata (the summary).
Results also contain a buffer that automatically stores unconsumed records when results are consumed out of order.

A :class:`.Result` is attached to an active connection, through a :class:`.Session`, until all its content has been buffered or consumed.

.. class:: .Result

    .. describe:: iter(result)

    .. autoattribute:: session

    .. automethod:: attached

    .. automethod:: detach

    .. automethod:: keys

    .. automethod:: records

    .. automethod:: summary

    .. automethod:: consume

    .. automethod:: single

    .. automethod:: peek

    .. automethod:: graph

    .. automethod:: value

    .. automethod:: values

    .. automethod:: data


.. class:: .Record

    A :class:`.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :py:class:`namedtuple` than to a
    :py:class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.

    .. describe:: Record(iterable)

        Create a new record based on an dictionary-like iterable.
        This can be a dictionary itself, or may be a sequence of key-value pairs, each represented by a tuple.

    .. describe:: record == other

        Compare a record for equality with another value.
        The `other` value may be any `Sequence` or `Mapping`, or both.
        If comparing with a `Sequence`, the values are compared in order.
        If comparing with a `Mapping`, the values are compared based on their keys.
        If comparing with a value that exhibits both traits, both comparisons must be true for the values to be considered equal.

    .. describe:: record != other

        Compare a record for inequality with another value.
        See above for comparison rules.

    .. describe:: hash(record)

        Create a hash for this record.
        This will raise a :exc:`TypeError` if any values within the record are unhashable.

    .. describe:: record[index]

        Obtain a value from the record by index.
        This will raise an :exc:`IndexError` if the specified index is out of range.

    .. describe:: record[i:j]

        Derive a sub-record based on a start and end index.
        All keys and values within those bounds will be copied across in the same order as in the original record.

    .. describe:: record[key]

        Obtain a value from the record by key.
        This will raise a :exc:`KeyError` if the specified key does not exist.

    .. automethod:: get(key, default=None)

    .. automethod:: value(key=0, default=None)

    .. automethod:: index(key)

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items

    .. automethod:: data


Summary Details
---------------

.. autoclass:: .ResultSummary
   :members:

.. autoclass:: .SummaryCounters
   :members:
