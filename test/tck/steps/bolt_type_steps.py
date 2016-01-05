from behave import *

from test.tck import tck_util

use_step_matcher("re")


@given("A running database")
def step_impl(context):
    return None
    # check if running


@given("a value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = tck_util.get_bolt_value(bolt_type, input)


@given("a value  of type (?P<bolt_type>.+)")
def step_impl(context, bolt_type):
    context.expected = tck_util.get_bolt_value(bolt_type, u' ')


@given("a list value (?P<input>.+) of type (?P<bolt_type>.+)")
def step_impl(context, input, bolt_type):
    context.expected = tck_util.get_list_from_feature_file(input, bolt_type)


@given("an empty list L")
def step_impl(context):
    context.L = []


@given("an empty map M")
def step_impl(context):
    context.M = {}


@given("a String of size (?P<size>\d+)")
def step_impl(context, size):
    context.expected = tck_util.get_random_string(int(size))


@given("a List of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = tck_util.get_list_of_random_type(int(size), type)


@given("a Map of size (?P<size>\d+) and type (?P<type>.+)")
def step_impl(context, size, type):
    context.expected = tck_util.get_dict_of_random_type(int(size), type)


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
        context.M['a%d' % len(context.M)] = tck_util.get_bolt_value(row[0], row[1])


@step("adding map M to list L")
def step_impl(context):
    context.L.append(context.M)


@when("adding a table of lists to the map M")
def step_impl(context):
    for row in context.table:
        context.M['a%d' % len(context.M)] = tck_util.get_list_from_feature_file(row[1], row[0])


@step("adding a copy of map M to map M")
def step_impl(context):
    context.M['a%d' % len(context.M)] = context.M.copy()


@when("the driver asks the server to echo this value back")
def step_impl(context):
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this list back")
def step_impl(context):
    context.expected = context.L
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@when("the driver asks the server to echo this map back")
def step_impl(context):
    context.expected = context.M
    context.results = {}
    context.results["as_string"] = tck_util.send_string("RETURN " + tck_util.as_cypher_text(context.expected))
    context.results["as_parameters"] = tck_util.send_parameters("RETURN {input}", {'input': context.expected})


@then("the result returned from the server should be a single record with a single value")
def step_impl(context):
    assert context.results
    for result in context.results.values():
        assert len(result) == 1
        assert len(result[0]) == 1


@step("the value given in the result should be the same as what was sent")
def step_impl(context):
    assert len(context.results) > 0
    for result in context.results.values():
        result_value = result[0].values()[0]
        assert result_value == context.expected