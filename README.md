# Official Python Client Driver for Neo4j

```python
import neo4j
session = neo4j.session("neo4j://localhost")
session.run("CREATE (a:Person {name:'Bob'})")
for name, in session.run("MATCH (a:Person) RETURN a.name AS name"):
    print(name)
session.close()
```


## Command Line

```python
python -m neo4j "CREATE (a:Person {name:'Alice'}) RETURN a, labels(a), a.name"
```


## Performance Testing

```bash
$ python -m neo4j.perftest "MATCH (a:Person {name:'Alice'}) RETURN a"
Running "MATCH (a:Person {name:'Alice'}) RETURN a" × 10000
  ×  1 process   ->  8671.316697 tx/s (new) ->   884.817077 tx/s (old)
  ×  2 processes -> 15673.956440 tx/s (new) ->  1640.076963 tx/s (old)
  ×  4 processes -> 16186.679656 tx/s (new) ->  2335.419501 tx/s (old)
  ×  8 processes -> 19158.285415 tx/s (new) ->  2757.685895 tx/s (old)
  × 16 processes -> 18091.494426 tx/s (new) ->  2678.465474 tx/s (old)
```


## Profiling

```bash
python -m cProfile -s cumtime profile/driver_profile.py | less
```
