import pytest

from neo4j.v1.orm import Node, Relationship
from neo4j.v1.orm.schema import Schema, Str, Int


@pytest.fixture(scope='function')
def UserNode():
    class User(Node):
        has_friend = Relationship('HAS_FRIEND', '->')
        friend_of = Relationship('FRIEND_OF', '<-')

        class Properties(Schema):
            colors = Str(many=True)
            name = Str()
            age = Int()

    return User
