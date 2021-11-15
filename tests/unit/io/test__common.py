import pytest

from neo4j.io._common import Outbox


@pytest.mark.parametrize(("chunk_size", "data", "result"), (
    (
        2,
        (bytes(range(10, 15)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13, 0, 1, 14))
    ),
    (
        2,
        (bytes(range(10, 14)),),
        bytes((0, 2, 10, 11, 0, 2, 12, 13))
    ),
    (
        2,
        (bytes((5, 6, 7)), bytes((8, 9))),
        bytes((0, 2, 5, 6, 0, 1, 7, 0, 2, 8, 9))
    ),
))
def test_outbox_chunking(chunk_size, data, result):
    outbox = Outbox(max_chunk_size=chunk_size)
    assert bytes(outbox.view()) == b""
    for d in data:
        outbox.write(d)
    assert bytes(outbox.view()) == result
    outbox.clear()
    assert bytes(outbox.view()) == b""
