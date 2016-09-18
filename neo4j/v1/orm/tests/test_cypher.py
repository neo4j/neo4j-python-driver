from neo4j.v1.orm import Cypher


def test_cypher_MATCH(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher = Cypher().match(u1)
    assert (cypher.emit() ==
            'MATCH (user_1:User)'
            )

    cypher = Cypher().match(u1, u2)
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_2:User)'
            )

    cypher = Cypher().match(u1.has_friend(u2))
    assert (cypher.emit() ==
            'MATCH (user_1:User)-[has_friend:HAS_FRIEND]->(user_2:User)'
            )

    cypher = Cypher().match(u1.has_friend(u2), u3)
    assert (cypher.emit() ==
            'MATCH (user_1:User)-[has_friend:HAS_FRIEND]->(user_2:User)'
            ', (user_3:User)'
            )


def test_cypher_MERGE(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher = Cypher().match(u1, u2).merge(u1.friend_of(u2))
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_2:User)\n'
            'MERGE (user_1:User)<-[friend_of:FRIEND_OF]-(user_2:User)'
            )

    cypher = Cypher().match(u1, u2)\
        .merge(u1.friend_of(u2), u3.friend_of(u1))
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_2:User)\n'
            'MERGE (user_1:User)<-[friend_of:FRIEND_OF]-(user_2:User)\n'
            'MERGE (user_3:User)<-[friend_of:FRIEND_OF]-(user_1:User)'
            )

    cypher = Cypher().match(u1, u2)\
        .merge(u1.friend_of(u2))\
        .merge(u3.friend_of(u1))
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_2:User)\n'
            'MERGE (user_1:User)<-[friend_of:FRIEND_OF]-(user_2:User)\n'
            'MERGE (user_3:User)<-[friend_of:FRIEND_OF]-(user_1:User)'
            )


def test_cypher_CREATE(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher = Cypher().match(u1, u2).create(u1.friend_of(u2), u3)
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_2:User)\n'
            'CREATE (user_1:User)<-[friend_of:FRIEND_OF]-(user_2:User)'
            ', (user_3:User)'
            )


def test_cypher_RETURN(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher = Cypher().match(u1, u3).ret(u1, u3)
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_3:User)\n'
            'RETURN user_1, user_3'
            )

    cypher = Cypher().match(u1, u3).ret(u1, u3).ret(u2)
    assert (cypher.emit() ==
            'MATCH (user_1:User), (user_3:User)\n'
            'RETURN user_1, user_3, user_2'
            )


def test_cypher_WHERE(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher = Cypher().match(u1).where(u1.name == 'Claire')
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name = "Claire"'
            )

    cypher = Cypher().match(u1).where(u1.name != 'Claire')
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name <> "Claire"'
            )

    cypher = Cypher().match(u1).where(u1.age < 17)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.age < 17'
            )

    cypher = Cypher().match(u1).where(u1.age > 17)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.age > 17'
            )

    cypher = Cypher().match(u1).where(u1.age >= 17)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.age >= 17'
            )

    cypher = Cypher().match(u1).where(u1.age <= 17)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.age <= 17'
            )

    cypher = Cypher().match(u1).where(u1.name == 'Claire', u1.age <= 17)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name = "Claire" '
            'AND user_1.age <= 17'
            )

    cypher = Cypher().match(u1).where(u1.name == u2.name)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name = user_2.name'
            )

    cypher = Cypher().match(u1)\
            .where(u1.name == u2.name, u1.name != u3.name)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name = user_2.name AND '
            'user_1.name <> user_3.name'
            )

    cypher = Cypher().match(u1)\
            .where(u1.name == u2.name)\
            .where(u1.name != u3.name)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\nWHERE user_1.name = user_2.name AND '
            'user_1.name <> user_3.name'
            )


def test_cypher_SKIP_LIMIT(UserNode):
    u1 = UserNode.symbol()

    cypher = Cypher().match(u1).skip(5)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\n'
            'SKIP 5'
            )

    cypher = Cypher().match(u1).limit(5)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\n'
            'LIMIT 5'
            )

    cypher = Cypher().match(u1).skip(1).limit(5)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\n'
            'SKIP 1\n'
            'LIMIT 5'
            )

    cypher = Cypher().match(u1).skip(1).limit(5).limit(10)
    assert (cypher.emit() ==
            'MATCH (user_1:User)\n'
            'SKIP 1\n'
            'LIMIT 10'
            )


def test_cypher_WITH(UserNode):
    u1 = UserNode.symbol()
    u2 = UserNode.symbol()
    u3 = UserNode.symbol()

    cypher1 = Cypher().match(u1, u3).ret(u1).ret(u3)
    cypher2 = Cypher(cypher1).match(u2).ret(u1, u2)
    assert (cypher2.emit() ==
            'MATCH (user_1:User), (user_3:User)\n'
            'WITH user_1, user_3\n'
            'MATCH (user_2:User)\n'
            'RETURN user_1, user_2'
            )
