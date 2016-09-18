def test_node_dump(UserNode):
    u = UserNode(properties={'name': 'Danielle', 'age': 16})
    assert u.dump() == {'name': 'Danielle', 'age': 16}


def test_node_load(UserNode):
    u = UserNode.load({'name': 'Danielle', 'age': 16})
    assert u['name'] == 'Danielle'
    assert u['age'] == 16
