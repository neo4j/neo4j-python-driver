def test_create_symbol_from_node(UserNode):
    s = UserNode.symbol()
    assert s.emit() == '(user_1:User)'


def test_symbol_customization(UserNode):
    s = UserNode.symbol(props={'id': 1})
    assert s.emit() == '(user_1:User {id: 1})'

    s = UserNode.symbol(props={'id': 2}, name='alda')
    assert s.emit() == '(alda:User {id: 2})'

    s = UserNode.symbol(props={'id': 3}, name='alda', label='Human')
    assert s.emit() == '(alda:Human {id: 3})'

    s = UserNode.symbol(name='claire')
    assert s(label='Female').emit() == '(claire:Female)'
    assert s(name='natasha').emit() == '(natasha:User)'

    s = UserNode.symbol(name=None)
    assert s.emit() == '(:User)'

    s = UserNode.symbol(name='marie', label=None)
    assert s.emit() == '(marie)'


def test_symbolic_relationship(UserNode):
    s1 = UserNode.symbol()
    s2 = UserNode.symbol()
    assert (s1.has_friend(s2).emit() ==
        '(user_1:User)-[has_friend:HAS_FRIEND]->(user_2:User)'
        )


def test_customized_symbolic_relationship(UserNode):
    s1 = UserNode.symbol()
    s2 = UserNode.symbol()

    assert (s1.has_friend(s2, label=None).emit() ==
        '(user_1:User)-[has_friend]->(user_2:User)'
        )

    assert (s1.has_friend(s2, props={'likes': 10}).emit() ==
        '(user_1:User)-[has_friend:HAS_FRIEND {likes: 10}]->(user_2:User)'
        )

    assert (s1.has_friend(s2, name=None).emit() ==
        '(user_1:User)-[:HAS_FRIEND]->(user_2:User)'
        )

    assert (s1.friend_of(s2, name=None).emit() ==
        '(user_1:User)<-[:FRIEND_OF]-(user_2:User)'
        )
