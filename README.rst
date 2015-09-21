============================
Neo4j Bolt Driver for Python
============================

.. code:: python

    import neo4j
    driver = neo4j.driver("bolt://localhost")
    session = driver.session()
    session.run("CREATE (a:Person {name:'Bob'})")
    for name, in session.run("MATCH (a:Person) RETURN a.name AS name"):
        print(name)
    session.close()


Command Line
============

.. code:: bash

    python -m neo4j "CREATE (a:Person {name:'Alice'}) RETURN a, labels(a), a.name"


Performance Testing
===================

.. code:: bash

    $ python -m neo4j.bench -p 8 "MATCH (a:Person {name:'Alice'}) RETURN a"
    Neo4j Benchmarking Tool for Python
    Copyright (c) 2002-2015 "Neo Technology,"
    Network Engine for Objects in Lund AB [http://neotechnology.com]
    Report bugs to nigel@neotechnology.com

    Python 3.4.1 (default, Nov 27 2014, 09:15:30)
    [GCC 4.8.2]

    This machine has 8 processors (linux)

    Latency measurements:

             INIT     <---SEND--->     <---RECV--->     DONE
    overall  <--------------------------------------------->
    network           <--------------------------->
    wait                          <--->

    INIT - start of overall request
    SEND - period of time over which data is sent
    RECV - period of time over which data is received
    DONE - end of overall request

    Using graph database at bolt://localhost
    Warming up... done

    ------------------------------------------------------
     MATCH (a:Person {name:'Alice'}) RETURN a
       × 10,000 runs × 8 clients = 2,601.3 tx/s
        (80,000 requests in 30.8s)
    ------------------------------------------------------
     percentile |   overall   |   network   |     wait
    ------------|-------------|-------------|-------------
           0.0% |   1,370.2µs |     199.0µs |       9.9µs
          10.0% |   2,673.3µs |     244.6µs |      18.3µs
          20.0% |   2,750.5µs |     270.2µs |      19.9µs
          30.0% |   2,790.2µs |     319.1µs |      21.9µs
          40.0% |   2,839.1µs |     331.8µs |     180.4µs
          50.0% |   2,921.7µs |     343.7µs |     197.1µs
          60.0% |   3,039.7µs |     355.8µs |     217.1µs
          70.0% |   3,106.4µs |     368.1µs |     283.9µs
          80.0% |   3,176.3µs |     382.8µs |     303.4µs
          90.0% |   3,334.3µs |     404.0µs |     329.8µs
          95.0% |   3,633.0µs |     541.3µs |     396.6µs
          98.0% |   4,888.2µs |   1,565.6µs |     954.5µs
          99.0% |   5,767.4µs |   2,585.5µs |   2,049.7µs
          99.5% |   6,810.7µs |   3,172.1µs |   2,846.9µs
          99.9% |  10,824.5µs |   5,846.7µs |   4,604.3µs
         100.0% |  29,740.3µs |  25,134.4µs |  25,065.5µs


Profiling
=========

.. code:: bash

    ./runprofile.sh "RETURN 1" | less


.. code:: bash
    ./runprofile.sh "RETURN 1" 20000 | less

