from behave import *

from test.tck import tck_util

from neo4j.v1.typesystem import Node, Relationship, Path

use_step_matcher("re")


@given("A running database")
def step_impl(context):
    return None
    # check if running


@given("a value (?P<Input>.+) of type (?P<BoltType>.+)")
def step_impl(context, Input, BoltType):
    context.expected = tck_util.get_bolt_value(BoltType, Input)


@given("a value  of type (?P<BoltType>.+)")
def step_impl(context, BoltType):
    context.expected = tck_util.get_bolt_value(BoltType, u' ')


@given("a list value (?P<Input>.+) of type (?P<BoltType>.+)")
def step_impl(context, Input, BoltType):
    context.expected = tck_util.get_list_from_feature_file(Input, BoltType)


@given("an empty list L")
def step_impl(context):
    context.L = []


@given("an empty map M")
def step_impl(context):
    context.M = {}


@given("an empty node N")
def step_impl(context):
    context.N = Node()


@given("a node N with properties and labels")
def step_impl(context):
    context.N = Node({"Person"}, {"name": "Alice", "age": 33})


@given("a String of size (?P<size>\d+)")
def step_impl(context, size):
    context.expected = tck_util.get_random_string(int(size))


@given("a List of size (?P<size>\d+) and type (?P<Type>.+)")
def step_impl(context, size, Type):
    context.expected = tck_util.get_list_of_random_type(int(size), Type)


@given("a Map of size (?P<size>\d+) and type (?P<Type>.+)")
def step_impl(context, size, Type):
    context.expected = tck_util.get_dict_of_random_type(int(size), Type)


@step("adding a table of lists to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(tck_util.get_list_from_feature_file(row[1], row[0]))


@step("adding a table of values to the list L")
def step_impl(context):
    for row in context.table:
        context.L.append(tck_util.get_bolt_value(row[0], row[1]))


@step("adding a table of values to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a' + str(len(context.M))] = tck_util.get_bolt_value(row[0], row[1])


@step("adding map M to list L")
def step_impl(context):
    context.L.append(context.M)


@when("adding a table of lists to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a' + str(len(context.M))] = tck_util.get_list_from_feature_file(row[1], row[0])


@step("adding a copy of map M to map M")
def step_impl(context):
    context.M['a' + str(len(context.M))] = context.M.copy()


@when("the driver asks the server to echo this value back")
def step_impl(context):
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypger_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this list back")
def step_impl(context):
    context.expected = context.L
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypger_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this map back")
def step_impl(context):
    context.expected = context.M
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypger_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this node back")
def step_impl(context):
    context.expected = context.N
    context.results = {}
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@then("the result returned from the server should be a single record with a single value")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        assert len(result) == 1
        assert len(result[0]) == 1


@step("the value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected


@step("the node value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected
        assert result_value.labels == context.expected.labels
        assert result_value.keys() == context.expected.keys()
        assert result_value.values() == context.expected.values()
        assert result_value.items() == context.expected.items()
        assert len(result_value) == len(context.expected)
        assert iter(result_value) == iter(context.expected)


##CURRENTLY NOT SUPPORTED IN PYTHON DRIVERS

@given("a relationship R")
def step_impl(context):
    alice = Node({"Person"}, {"name": "Alice", "age": 33})
    bob = Node({"Person"}, {"name": "Bob", "age": 44})
    context.R = Relationship(alice, bob, "KNOWS", {"since": 1999})


@when("the driver asks the server to echo this relationship R back")
def step_impl(context):
    context.expected = context.R
    context.results = {}
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@step("the relationship value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected
        assert result_value.start == context.expected.start
        assert result_value.type == context.expected.type
        assert result_value.end == context.expected.end
        assert result_value.items() == context.expected.items()
        assert result_value.keys() == context.expected.keys()
        assert result_value.values() == context.expected.values()


@given("a zero length path P")
def step_impl(context):
    context.P = Path(Node({"Person"}, {"name": "Alice", "age": 33}))


@given("a arbitrary long path P")
def step_impl(context):
    alice = Node({"Person"}, {"name": "Alice", "age": 33})
    bob = Node({"Person"}, {"name": "Bob", "age": 44})
    carol = Node({"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = Relationship(alice, bob, "KNOWS", {"since": 1999})
    carol_dislikes_bob = Relationship(carol, bob, "DISLIKES")
    context.P = Path(alice, alice_knows_bob, bob, carol_dislikes_bob, carol)


@when("the driver asks the server to echo this path back")
def step_impl(context):
    context.expected = context.P
    context.results = {}
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@step("the path value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected
        assert result_value.start == context.expected.start
        assert result_value.end == context.expected.end
        assert result_value.nodes == context.expected.nodes
        assert result_value.relationships == context.expected.relationships
        assert list(result_value) == list(context.expected)


@given("a Node with great amount of properties and labels")
def step_impl(context):
    context.N = Node(tck_util.get_list_of_random_type(1000, "String"),
                     tck_util.get_dict_of_random_type(1000, "String"))


@given("a path P of size (?P<size>\d+)")
def step_impl(context, size):
    nodes_and_rels = [Node({tck_util.get_random_string(5)}, tck_util.get_dict_of_random_type(3, "String"))]
    for i in range(1, int(12)):
        n = nodes_and_rels.append(Node({tck_util.get_random_string(5)}, tck_util.get_dict_of_random_type(3, "String")))
        r = Relationship(nodes_and_rels[-1], n, tck_util.get_random_string(4),
                         tck_util.get_dict_of_random_type(3, "String"))
        nodes_and_rels.append(r)
        nodes_and_rels.append(n)

    context.P = Path(nodes_and_rels[0], nodes_and_rels[1:])
