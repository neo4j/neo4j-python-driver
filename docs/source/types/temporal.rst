===================
Temporal Data Types
===================

.. py:currentmodule:: neotime

=============  ========  ==============  ======================================
Cypher         Property  Array Property  Python
=============  ========  ==============  ======================================
Date           *yes*     *yes*           ``neotime.Date``
Time           *yes*     *yes*           ``neotime.Time`` (tzinfo != None)
LocalTime      *yes*     *yes*           ``neotime.Time`` (tzinfo == None)
DateTime       *yes*     *yes*           ``neotime.DateTime`` (tzinfo != None)
LocalDateTime  *yes*     *yes*           ``neotime.DateTime`` (tzinfo == None)
Duration       *yes*     *yes*           ``neotime.Duration`` :sup:`[1]`
=============  ========  ==============  ======================================

.. admonition:: Notes

   1. A ``datetime.timespan`` value passed as a parameter will always be implicitly converted to a :class:`.Duration` value.

.. class:: Duration

.. class:: Date

.. class:: Time

.. class:: DateTime
