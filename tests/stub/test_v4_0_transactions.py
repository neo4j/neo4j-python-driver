from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
)

from neo4j import GraphDatabase, BoltDriver

from tests.stub.conftest import StubTestCase, StubCluster
from logging import getLogger

log = getLogger("neo4j")


class BoltDriverTestCase(StubTestCase):

    def test_transaction(self):
        # python -m pytest tests/stub/test_v4_0_transactions.py -s -k test_transaction
        with StubCluster("v4/transaction.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, max_retry_time=0, user_agent="test") as driver:
                with driver.session() as session:
                    tx = session.begin_transaction(database="system", mode="r", timeout=123)
                    result = tx.run("MATCH (n:Node) RETURN n.name as name")
                    #log.debug(dir(result.summary()))
                    assert result.keys() == ["name"]
                    # log.debug(dir(result))
                    records = list(result)
                    assert records == [["A"], ["B"], ["C"]]
                    # log.debug(result.summary())
                    tx.commit()

    def test_autocommit(self):
        # python -m pytest tests/stub/test_v4_0_transactions.py -s -k test_autocommit
        with StubCluster("v4/autocommit.script"):
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, max_retry_time=0, user_agent="test") as driver:
                with driver.session() as session:
                    # tx = session.begin_transaction(database="system", mode="r", timeout=123)
                    # result = session.run("MATCH (n:Node) RETURN n.name as name", {}, {"database": "system", "mode": "r", "timeout": 123})
                    result = session.run("MATCH (n:Node) RETURN n.name as name", {})
                    # Message mismatch (expected <RUN "MATCH (n:Node) RETURN n.name as name" {} {"mode": "r", "db": "system", "tx_timeout": 123}>, received <RUN "MATCH (n:Node) RETURN n.name as name" {} {}>)
                    # log.debug(result.summary())
                    assert result.keys() == ["name"]
                    # log.debug(dir(result))
                    records = list(result)
                    assert records == [["A"], ["B"], ["C"]]
                    # log.debug(result.summary())

    def test_autocommit_summary(self):
        # python -m pytest tests/stub/test_v4_0_transactions.py -s -k test_autocommit_summary
        with StubCluster("v4/autocommit_summary.script"):
            # FIX: C: RUN "CREATE (n:Tester) {'name': $name} RETURN n.name AS name" {"name": "test"} {"db": "system", "mode": "w", "tx_timeout": 123}

            # Breaks with:
            # SUCCESS {"bookmark": "bookmark:1:autocommit", "t_last": "12345", "type": "w", "stats": {"nodes_created": 1}, "plan": {"operatorType": "undefined"}, "profile": {"operatorType": "undefined"}, "notifications": {}, "db": "system"}
            uri = "bolt://127.0.0.1:9001"
            with GraphDatabase.driver(uri, auth=self.auth_token, max_retry_time=0, user_agent="test") as driver:
                with driver.session() as session:
                    result = session.run(cypher="CREATE (n:Tester) {name: $name} RETURN n.name AS name", parameters={"name": "test"}, database="system", mode="w", timeout=123, fetch_size=1000)
                    summary = result.summary()
                    assert summary.statement == "CREATE (n:Tester) {name: $name} RETURN n.name AS name"
                    assert summary.parameters == {"name": "test"}
                    assert summary.statement_type == "w"
                    assert summary.counters.nodes_created == 1

