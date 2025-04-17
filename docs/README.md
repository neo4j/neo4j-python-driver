====================
Sphinx Documentation
====================

Building the docs requires Python 3.10+

In project root
```
pip install -r requirements-dev.txt
```

In this directory
```
make -C docs html
```

```
python -m sphinx -b html docs/source build/html
```
