#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pytest

from neo4j import (
    GraphDatabase,
    Neo4jDriver,
)
from neo4j.api import READ_ACCESS, WRITE_ACCESS

from neo4j._exceptions import BoltRoutingError

from neo4j.exceptions import (
    ServiceUnavailable,
    ClientError,
    TransientError,
    SessionExpired,
)

from tests.stub.conftest import StubCluster

# python -m pytest tests/stub/test_routingdriver.py -s -v


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router.script",
    ]
)
def test_bolt_plus_routing_uri_constructs_neo4j_driver(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_bolt_plus_routing_uri_constructs_neo4j_driver
    with StubCluster(test_script):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            assert isinstance(driver, Neo4jDriver)


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router_port_9001_one_read_port_9004_one_write_port_9006.script",
    ]
)
def test_neo4j_driver_verify_connectivity(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_neo4j_driver_verify_connectivity
    uri = "bolt+routing://127.0.0.1:9001"
    with StubCluster(test_script):
        driver = GraphDatabase.driver(uri, auth=driver_info["auth_token"], user_agent="test")
        assert isinstance(driver, Neo4jDriver)

    with StubCluster(test_script):
        assert driver.verify_connectivity() is not None
        driver.close()


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router_port_9001_one_read_port_9004_one_write_port_9006.script",
    ]
)
def test_neo4j_driver_verify_connectivity_server_down(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_neo4j_driver_verify_connectivity_server_down
    uri = "bolt+routing://127.0.0.1:9001"
    with StubCluster(test_script):
        driver = GraphDatabase.driver(uri, auth=driver_info["auth_token"], user_agent="test")
        assert isinstance(driver, Neo4jDriver)

        with pytest.raises(ServiceUnavailable):
            driver.verify_connectivity()

        driver.close()


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/non_router.script",
        "v4x0/routing_table_failure_not_a_router.script",
    ]
)
def test_cannot_discover_servers_on_non_router(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_cannot_discover_servers_on_non_router
    with StubCluster(test_script):
        uri = "bolt+routing://127.0.0.1:9001"
        with pytest.raises(ServiceUnavailable):
            with GraphDatabase.driver(uri, auth=driver_info["auth_token"]):
                pass


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/silent_router.script",
        "v4x0/routing_table_silent_router.script",
    ]
)
def test_cannot_discover_servers_on_silent_router(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_cannot_discover_servers_on_silent_router
    with StubCluster(test_script):
        uri = "bolt+routing://127.0.0.1:9001"
        with pytest.raises(BoltRoutingError):
            with GraphDatabase.driver(uri, auth=driver_info["auth_token"]):
                pass


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router.script",
        "v4x0/router.script",
    ]
)
def test_should_discover_servers_on_driver_construction(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_discover_servers_on_driver_construction
    with StubCluster(test_script):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            table = driver._pool.routing_table
            assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002),
                                     ('127.0.0.1', 9003)}
            assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
            assert table.writers == {('127.0.0.1', 9006)}


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/return_1.script"),
        ("v4x0/router.script", "v4x0/return_1_port_9004.script"),
    ]
)
def test_should_be_able_to_read(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_be_able_to_read
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result = session.run("RETURN $x", {"x": 1})
                for record in result:
                    assert record["x"] == 1
                assert result.summary().server.address == ('127.0.0.1', 9004)


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/create_a.script"),
        ("v4x0/router.script", "v4x0/create_test_node_port_9006.script"),
    ]
)
def test_should_be_able_to_write(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_be_able_to_write
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=WRITE_ACCESS) as session:
                result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                assert not list(result)
                assert result.summary().server.address == ('127.0.0.1', 9006)


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/create_a.script"),
        ("v4x0/router.script", "v4x0/create_test_node_port_9006.script"),
    ]
)
def test_should_be_able_to_write_as_default(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_be_able_to_write_as_default
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session() as session:
                result = session.run("CREATE (a $x)", {"x": {"name": "Alice"}})
                assert not list(result)
                assert result.summary().server.address == ('127.0.0.1', 9006)


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/disconnect_on_run_9004.script"),
        ("v4x0/router.script", "v4x0/disconnect_on_run_port_9004.script"),
    ]
)
def test_routing_disconnect_on_run(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_routing_disconnect_on_run
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with pytest.raises(SessionExpired):
                with driver.session(default_access_mode=READ_ACCESS) as session:
                    session.run("RETURN $x", {"x": 1}).consume()


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/disconnect_on_pull_all_9004.script"),
        ("v4x0/router.script", "v4x0/disconnect_on_pull_port_9004.script"),
    ]
)
def test_routing_disconnect_on_pull_all(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_routing_disconnect_on_pull_all
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with pytest.raises(SessionExpired):
                with driver.session(default_access_mode=READ_ACCESS) as session:
                    session.run("RETURN $x", {"x": 1}).consume()


@pytest.mark.parametrize(
    "test_scripts",
    [
        ("v3/router.script", "v3/return_1.script"),
        ("v4x0/router.script", "v4x0/return_1_port_9004.script"),
    ]
)
def test_should_disconnect_after_fetching_autocommit_result(driver_info, test_scripts):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_disconnect_after_fetching_autocommit_result
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result = session.run("RETURN $x", {"x": 1})
                assert session._connection is not None
                result.consume()
                assert session._connection is None


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/return_1_twice_in_read_tx.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router.script", "v4x0/tx_return_1_twice_port_9004.script"), ("RETURN 1", )),
    ]
)
def test_should_disconnect_after_explicit_commit(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_disconnect_after_explicit_commit
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                with session.begin_transaction() as tx:
                    result = tx.run(*test_run_args)
                    assert session._connection is not None
                    result.consume()
                    assert session._connection is not None
                    result = tx.run(*test_run_args)
                    assert session._connection is not None
                    result.consume()
                    assert session._connection is not None
                assert session._connection is None


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/return_1_twice.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router.script", "v4x0/return_1_twice_port_9004.script"), ("RETURN 1", )),
    ]
)
def test_should_reconnect_for_new_query(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_reconnect_for_new_query
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result_1 = session.run(*test_run_args)
                assert session._connection is not None
                result_1.consume()
                assert session._connection is None
                result_2 = session.run(*test_run_args)
                assert session._connection is not None
                result_2.consume()
                assert session._connection is None


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/return_1_twice.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router.script", "v4x0/return_1_twice_port_9004.script"), ("RETURN 1", )),
    ]
)
def test_should_retain_connection_if_fetching_multiple_results(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_retain_connection_if_fetching_multiple_results
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result_1 = session.run(*test_run_args)
                result_2 = session.run(*test_run_args)
                assert session._connection is not None
                result_1.consume()
                assert session._connection is not None
                result_2.consume()
                assert session._connection is None


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/return_1_four_times.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router.script", "v4x0/return_1_four_times_port_9004.script"), ("RETURN 1", )),
    ]
)
def test_two_sessions_can_share_a_connection(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_two_sessions_can_share_a_connection
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            session_1 = driver.session(default_access_mode=READ_ACCESS)
            session_2 = driver.session(default_access_mode=READ_ACCESS)

            result_1a = session_1.run(*test_run_args)
            c = session_1._connection
            result_1a.consume()

            result_2a = session_2.run(*test_run_args)
            assert session_2._connection is c
            result_2a.consume()

            result_1b = session_1.run(*test_run_args)
            assert session_1._connection is c
            result_1b.consume()

            result_2b = session_2.run(*test_run_args)
            assert session_2._connection is c
            result_2b.consume()

            session_2.close()
            session_1.close()


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/get_routing_table.script", "v3/return_1_on_9002.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router_role_route_share_port_with_role_read_and_role_write.script", "v4x0/return_1_port_9002.script"), ("RETURN 1 AS x", )),
    ]
)
def test_should_call_get_routing_table_procedure(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_call_get_routing_table_procedure
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result = session.run(*test_run_args)
                for record in result:
                    assert record["x"] == 1
                assert result.summary().server.address == ('127.0.0.1', 9002)


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/get_routing_table_with_context.script", "v3/return_1_on_9002.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router_get_routing_table_with_context.script", "v4x0/return_1_port_9002.script"), ("RETURN 1 AS x", )),
    ]
)
def test_should_call_get_routing_table_with_context(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_call_get_routing_table_with_context
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001/?name=molly&age=1"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result = session.run(*test_run_args)
                for record in result:
                    assert record["x"] == 1
                assert result.summary().server.address == ('127.0.0.1', 9002)


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router_no_writers.script", "v3/return_1_on_9005.script"), ("RETURN $x", {"x": 1})),
        (("v4x0/router_with_no_role_write.script", "v4x0/return_1_port_9005.script"), ("RETURN 1 AS x", )),
    ]
)
def test_should_serve_read_when_missing_writer(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_serve_read_when_missing_writer
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:
                result = session.run(*test_run_args)
                for record in result:
                    assert record["x"] == 1
                assert result.summary().server.address == ('127.0.0.1', 9005)


@pytest.mark.parametrize(
    "test_script",
    [
        "v3/router_no_readers.script",
        "v4x0/router_with_no_role_read.script",
    ]
)
def test_should_error_when_missing_reader(driver_info, test_script):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_should_error_when_missing_reader
    with StubCluster(test_script):
        uri = "bolt+routing://127.0.0.1:9001"
        with pytest.raises(BoltRoutingError):
            GraphDatabase.driver(uri, auth=driver_info["auth_token"])


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/not_a_leader.script"), ("CREATE (n {name:'Bob'})", )),
        (("v4x0/router.script", "v4x0/run_with_failure_cluster_not_a_leader.script"), ("CREATE (n:TEST {name:'test'})", )),
    ]
)
def test_forgets_address_on_not_a_leader_error(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_forgets_address_on_not_a_leader_error
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=WRITE_ACCESS) as session:
                with pytest.raises(ClientError):
                    _ = session.run(*test_run_args)

                pool = driver._pool
                table = pool.routing_table

                # address might still have connections in the pool, failed instance just can't serve writes
                assert ('127.0.0.1', 9006) in pool.connections
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                # writer 127.0.0.1:9006 should've been forgotten because of an error
                assert len(table.writers) == 0


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/forbidden_on_read_only_database.script"), ("CREATE (n {name:'Bob'})", )),
        (("v4x0/router.script", "v4x0/run_with_failure_forbidden_on_read_only_database.script"), ("CREATE (n:TEST {name:'test'})", )),
    ]
)
def test_forgets_address_on_forbidden_on_read_only_database_error(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_forgets_address_on_forbidden_on_read_only_database_error
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=WRITE_ACCESS) as session:
                with pytest.raises(ClientError):
                    _ = session.run(*test_run_args)

                pool = driver._pool
                table = pool.routing_table

                # address might still have connections in the pool, failed instance just can't serve writes
                assert ('127.0.0.1', 9006) in pool.connections
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                assert table.readers == {('127.0.0.1', 9004), ('127.0.0.1', 9005)}
                # writer 127.0.0.1:9006 should've been forgotten because of an error
                assert len(table.writers) == 0


@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/rude_reader.script"), ("RETURN 1", )),
        (("v4x0/router.script", "v4x0/disconnect_on_pull_port_9004.script"), ("RETURN $x", {"x": 1})),
    ]
)
def test_forgets_address_on_service_unavailable_error(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_forgets_address_on_service_unavailable_error
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:

                pool = driver._pool
                table = pool.routing_table
                table.readers.remove(('127.0.0.1', 9005))

                with pytest.raises(SessionExpired):
                    _ = session.run(*test_run_args)

                # address should have connections in the pool but be inactive, it has failed
                assert ('127.0.0.1', 9004) in pool.connections
                conns = pool.connections[('127.0.0.1', 9004)]
                conn = conns[0]
                assert conn._closed is True
                assert conn.in_use is True
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                # reader 127.0.0.1:9004 should've been forgotten because of an error
                assert not table.readers
                assert table.writers == {('127.0.0.1', 9006)}

            assert conn.in_use is False

@pytest.mark.parametrize(
    "test_scripts, test_run_args",
    [
        (("v3/router.script", "v3/database_unavailable.script"), ("RETURN 1", )),
        (("v4x0/router.script", "v4x0/run_with_failure_database_unavailable.script"), ("RETURN 1", )),
    ]
)
def test_forgets_address_on_database_unavailable_error(driver_info, test_scripts, test_run_args):
    # python -m pytest tests/stub/test_routingdriver.py -s -v -k test_forgets_address_on_database_unavailable_error
    with StubCluster(*test_scripts):
        uri = "bolt+routing://127.0.0.1:9001"
        with GraphDatabase.driver(uri, auth=driver_info["auth_token"]) as driver:
            with driver.session(default_access_mode=READ_ACCESS) as session:

                pool = driver._pool
                table = pool.routing_table
                table.readers.remove(('127.0.0.1', 9005))

                with pytest.raises(TransientError) as raised:
                    _ = session.run(*test_run_args)
                    assert raised.exception.title == "DatabaseUnavailable"

                pool = driver._pool
                table = pool.routing_table

                # address should not have connections in the pool, it has failed
                assert ('127.0.0.1', 9004) not in pool.connections
                assert table.routers == {('127.0.0.1', 9001), ('127.0.0.1', 9002), ('127.0.0.1', 9003)}
                # reader 127.0.0.1:9004 should've been forgotten because of an raised
                assert not table.readers
                assert table.writers == {('127.0.0.1', 9006)}
