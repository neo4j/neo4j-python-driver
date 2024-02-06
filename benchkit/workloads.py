from __future__ import annotations

import asyncio
import enum
import typing as t
from dataclasses import dataclass
from typing import Iterator

import typing_extensions as te

import neo4j
from neo4j import (
    AsyncDriver,
    AsyncManagedTransaction,
    AsyncSession,
    Record,
)


__all__ = [
    "Workloads",
    "Workload",
]


class Workloads(t.Mapping):
    def __init__(self) -> None:
        self._workloads: t.Dict[str, Workload] = {}
        self._current_id: int = 0

    def store_workload(self, data: t.Any) -> str:
        name = str(self._current_id + 1)
        self._workloads[str(name)] = Workload(data)
        self._current_id += 1
        return name

    @classmethod
    def parse_workload(cls, data: t.Any) -> Workload:
        return Workload(data)

    def __getitem__(self, name: str) -> Workload:
        return self._workloads[name]

    def __len__(self) -> int:
        return len(self._workloads)

    def __iter__(self) -> Iterator[str]:
        return iter(self._workloads)

    def __delitem__(self, name: str) -> None:
        del self._workloads[name]


class Workload:
    _method: _WorkloadMethod
    _queries: t.List[_WorkloadQuery]
    _config: _WorkloadConfig

    def __init__(self, data: t.Any) -> None:
        if not isinstance(data, dict):
            raise TypeError("Workload data must be a dict")
        self._method = _WorkloadMethod.parse(data)
        if "queries" not in data:
            raise ValueError("Workload data must have queries")
        self._queries = _WorkloadQuery.parse_multiple(data["queries"])
        self._config = _WorkloadConfig.parse(data)

    def patch(self, data: t.Any) -> None:
        if not isinstance(data, dict):
            raise TypeError("Workload data must be a dict")
        if "method" in data or "mode" in data:
            self._method = self._method.patched(data)
        if "queries" in data:
            self._queries = _WorkloadQuery.parse_multiple(data["queries"])
        self._config.patch(data)

    def __call__(self, driver: AsyncDriver) -> t.Awaitable[None]:
        return self._run(driver)

    async def _run(self, driver: AsyncDriver) -> None:
        await self._method.prepare()(driver, self._queries, self._config)


class _WorkloadMethod(enum.Enum):
    EXECUTE_QUERY_PARALLEL_SESSIONS = enum.auto()
    EXECUTE_QUERY_SEQUENTIAL_SESSIONS = enum.auto()

    SESSION_RUN_PARALLEL_SESSIONS = enum.auto()
    SESSION_RUN_SEQUENTIAL_SESSIONS = enum.auto()
    SESSION_RUN_SEQUENTIAL_TRANSACTIONS = enum.auto()

    EXECUTE_READ_PARALLEL_SESSIONS = enum.auto()
    EXECUTE_READ_SEQUENTIAL_SESSIONS = enum.auto()
    EXECUTE_READ_SEQUENTIAL_TRANSACTIONS = enum.auto()
    EXECUTE_READ_SEQUENTIAL_QUERIES = enum.auto()

    EXECUTE_WRITE_PARALLEL_SESSIONS = enum.auto()
    EXECUTE_WRITE_SEQUENTIAL_SESSIONS = enum.auto()
    EXECUTE_WRITE_SEQUENTIAL_TRANSACTIONS = enum.auto()
    EXECUTE_WRITE_SEQUENTIAL_QUERIES = enum.auto()

    @classmethod
    def parse(cls, data: t.Any) -> _WorkloadMethod:
        if "method" not in data:
            raise ValueError("Workload data must have a method")
        mode = data.get("mode", "sequentialSessions")
        if not isinstance(mode, str):
            raise TypeError("Workload mode must be a string")
        method = data["method"]
        if method == "executeQuery":
            if mode == "parallelSessions":
                return cls.EXECUTE_QUERY_PARALLEL_SESSIONS
            elif mode == "sequentialSessions":
                return cls.EXECUTE_QUERY_SEQUENTIAL_SESSIONS
            else:
                raise ValueError(
                    f"Unknown workload mode for executeQuery: {mode}. "
                    f"Must be one of: 'sequentialSessions', "
                    f"'parallelSessions'."
                )
        elif method == "sessionRun":
            if mode == "parallelSessions":
                return cls.SESSION_RUN_PARALLEL_SESSIONS
            elif mode == "sequentialSessions":
                return cls.SESSION_RUN_SEQUENTIAL_SESSIONS
            elif mode == "sequentialTransactions":
                return cls.SESSION_RUN_SEQUENTIAL_TRANSACTIONS
            else:
                raise ValueError(
                    f"Unknown workload mode for sessionRun: {mode}. "
                    f"Must be one of: 'sequentialSessions', "
                    f"'parallelSessions', 'sequentialTransactions'."
                )
        elif method == "executeRead":
            if mode == "parallelSessions":
                return cls.EXECUTE_READ_PARALLEL_SESSIONS
            elif mode == "sequentialSessions":
                return cls.EXECUTE_READ_SEQUENTIAL_SESSIONS
            elif mode == "sequentialTransactions":
                return cls.EXECUTE_READ_SEQUENTIAL_TRANSACTIONS
            elif mode == "sequentialQueries":
                return cls.EXECUTE_READ_SEQUENTIAL_QUERIES
            else:
                raise ValueError(
                    f"Unknown workload mode for executeRead: {mode}. "
                    f"Must be one of: 'sequentialSessions', "
                    f"'parallelSessions', 'sequentialTransactions', "
                    f"'sequentialQueries'."
                )
        elif method == "executeWrite":
            if mode == "parallelSessions":
                return cls.EXECUTE_WRITE_PARALLEL_SESSIONS
            elif mode == "sequentialSessions":
                return cls.EXECUTE_WRITE_SEQUENTIAL_SESSIONS
            elif mode == "sequentialTransactions":
                return cls.EXECUTE_WRITE_SEQUENTIAL_TRANSACTIONS
            elif mode == "sequentialQueries":
                return cls.EXECUTE_WRITE_SEQUENTIAL_QUERIES
            else:
                raise ValueError(
                    f"Unknown workload mode for executeWrite: {mode}. "
                    f"Must be one of: 'sequentialSessions', "
                    f"'parallelSessions', 'sequentialTransactions', "
                    f"'sequentialQueries'."
                )
        else:
            raise ValueError(
                f"Unknown workload method: {method}. "
                f"Must be one of: 'executeQuery', 'sessionRun', "
                f"'executeRead', 'executeWrite'."
            )

    def _to_data(self) -> dict:
        data = {}
        if self in (
            _WorkloadMethod.EXECUTE_QUERY_PARALLEL_SESSIONS,
            _WorkloadMethod.EXECUTE_QUERY_SEQUENTIAL_SESSIONS,
        ):
            data["method"] = "executeQuery"
        elif self in (
            _WorkloadMethod.SESSION_RUN_PARALLEL_SESSIONS,
            _WorkloadMethod.SESSION_RUN_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.SESSION_RUN_SEQUENTIAL_TRANSACTIONS,
        ):
            data["method"] = "sessionRun"
        elif self in (
            _WorkloadMethod.EXECUTE_READ_PARALLEL_SESSIONS,
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_TRANSACTIONS,
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_QUERIES,
        ):
            data["method"] = "executeRead"
        elif self in (
            _WorkloadMethod.EXECUTE_WRITE_PARALLEL_SESSIONS,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_TRANSACTIONS,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_QUERIES,
        ):
            data["method"] = "executeWrite"
        else:
            raise NotImplementedError(f"Unhandled workload method: {self}")
        if self in (
            _WorkloadMethod.EXECUTE_QUERY_PARALLEL_SESSIONS,
            _WorkloadMethod.SESSION_RUN_PARALLEL_SESSIONS,
            _WorkloadMethod.EXECUTE_READ_PARALLEL_SESSIONS,
            _WorkloadMethod.EXECUTE_WRITE_PARALLEL_SESSIONS,
        ):
            data["mode"] = "parallelSessions"
        elif self in (
            _WorkloadMethod.EXECUTE_QUERY_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.SESSION_RUN_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_SESSIONS,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_SESSIONS,
        ):
            data["mode"] = "sequentialSessions"
        elif self in (
            _WorkloadMethod.SESSION_RUN_SEQUENTIAL_TRANSACTIONS,
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_TRANSACTIONS,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_TRANSACTIONS,
        ):
            data["mode"] = "sequentialTransactions"
        elif self in (
            _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_QUERIES,
            _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_QUERIES,
        ):
            data["mode"] = "sequentialQueries"
        else:
            raise NotImplementedError(f"Unhandled workload mode: {self}")
        return data

    def patched(self, new_data: t.Any) -> _WorkloadMethod:
        if "mode" in new_data and "method" in new_data:
            return self.parse(new_data)
        else:
            data = self._to_data()
            if "mode" in new_data:
                data["mode"] = new_data["mode"]
            if "method" in new_data:
                data["method"] = new_data["method"]
            return self.parse(data)

    def prepare(
        self,
    ) -> t.Callable[
        [AsyncDriver, t.List[_WorkloadQuery], _WorkloadConfig],
        t.Awaitable[None]
    ]:
        if self == _WorkloadMethod.EXECUTE_QUERY_PARALLEL_SESSIONS:
            return self._execute_query_parallel_sessions
        elif self == _WorkloadMethod.EXECUTE_QUERY_SEQUENTIAL_SESSIONS:
            return self._execute_query_sequential_sessions
        elif self == _WorkloadMethod.SESSION_RUN_PARALLEL_SESSIONS:
            return self._session_run_parallel_sessions
        elif self == _WorkloadMethod.SESSION_RUN_SEQUENTIAL_SESSIONS:
            return self._session_run_sequential_sessions
        elif self == _WorkloadMethod.SESSION_RUN_SEQUENTIAL_TRANSACTIONS:
            return self._session_run_sequential_transactions
        elif self == _WorkloadMethod.EXECUTE_READ_PARALLEL_SESSIONS:
            return self._execute_read_parallel_sessions
        elif self == _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_SESSIONS:
            return self._execute_read_sequential_sessions
        elif self == _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_TRANSACTIONS:
            return self._execute_read_sequential_transactions
        elif self == _WorkloadMethod.EXECUTE_READ_SEQUENTIAL_QUERIES:
            return self._execute_read_sequential_queries
        elif self == _WorkloadMethod.EXECUTE_WRITE_PARALLEL_SESSIONS:
            return self._execute_write_parallel_sessions
        elif self == _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_SESSIONS:
            return self._execute_write_sequential_sessions
        elif self == _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_TRANSACTIONS:
            return self._execute_write_sequential_transactions
        elif self == _WorkloadMethod.EXECUTE_WRITE_SEQUENTIAL_QUERIES:
            return self._execute_write_sequential_queries
        else:
            raise NotImplementedError(f"Unhandled workload method: {self}")

    @classmethod
    async def _execute_query(
        cls,
        driver: AsyncDriver,
        query: _WorkloadQuery,
        config: _WorkloadConfig
    ) -> None:
        await driver.execute_query(
            query.query,
            parameters_=query.parameters,
            routing_=config.routing,
            database_=config.database,
        )

    @classmethod
    async def _execute_query_parallel_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        await asyncio.gather(
            *(
                cls._execute_query(driver, query, config)
                for query in queries
            ),
            return_exceptions=True,
        )

    @classmethod
    async def _execute_query_sequential_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        for query in queries:
            await cls._execute_query(driver, query, config)

    @classmethod
    async def _session_run(
        cls,
        driver: AsyncDriver,
        query: _WorkloadQuery,
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(
            default_access_mode=config.routing,
            database=config.database,
        ) as session:
            res = await session.run(query.query, parameters=query.parameters)
            _ = [record async for record in res]

    @classmethod
    async def _session_run_parallel_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        await asyncio.gather(
            *(
                cls._session_run(driver, query, config)
                for query in queries
            ),
            return_exceptions=True,
        )

    @classmethod
    async def _session_run_sequential_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        for query in queries:
            await cls._session_run(driver, query, config)

    @classmethod
    async def _session_run_sequential_transactions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(
            database=config.database,
            default_access_mode=config.routing
        ) as session:
            for query in queries:
                res = await session.run(query.query,
                                        parameters=query.parameters)
                _ = [record async for record in res]

    @classmethod
    async def _work(
        cls,
        tx: AsyncManagedTransaction,
        query: _WorkloadQuery,
    ) -> t.List[Record]:
        res = await tx.run(query.query, parameters=query.parameters)
        return [record async for record in res]

    @classmethod
    async def _work_sequential(
        cls,
        tx: AsyncManagedTransaction,
        queries: t.List[_WorkloadQuery],
    ) -> t.List[t.List[Record]]:
        return [await cls._work(tx, query) for query in queries]

    @classmethod
    async def _execute_read(
        cls,
        driver: AsyncDriver,
        query: _WorkloadQuery,
        config: _WorkloadConfig
    ) -> t.List[Record]:
        async with driver.session(database=config.database) as session:
            return await session.execute_read(cls._work, query)

    @classmethod
    async def _execute_read_parallel_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        await asyncio.gather(
            *(
                cls._execute_read(driver, query, config)
                for query in queries
            ),
            return_exceptions=True,
        )

    @classmethod
    async def _execute_read_sequential_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        _ = [
            await cls._execute_read(driver, query, config)
            for query in queries
        ]

    @classmethod
    async def _execute_read_sequential_transactions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(database=config.database) as session:
            _ = [
                await session.execute_read(cls._work, query)
                for query in queries
            ]

    @classmethod
    async def _execute_read_sequential_queries(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(database=config.database) as session:
            await session.execute_read(cls._work_sequential, queries)

    @classmethod
    async def _execute_write(
        cls,
        driver: AsyncDriver,
        query: _WorkloadQuery,
        config: _WorkloadConfig
    ) -> t.List[Record]:
        async with driver.session(database=config.database) as session:
            return await session.execute_write(cls._work, query)

    @classmethod
    async def _execute_write_parallel_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        await asyncio.gather(
            *(
                cls._execute_write(driver, query, config)
                for query in queries
            ),
            return_exceptions=True,
        )

    @classmethod
    async def _execute_write_sequential_sessions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        _ = [
            await cls._execute_write(driver, query, config)
            for query in queries
        ]

    @classmethod
    async def _execute_write_sequential_transactions(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(database=config.database) as session:
            _ = [
                await session.execute_write(cls._work, query)
                for query in queries
            ]

    @classmethod
    async def _execute_write_sequential_queries(
        cls,
        driver: AsyncDriver,
        queries: t.List[_WorkloadQuery],
        config: _WorkloadConfig
    ) -> None:
        async with driver.session(database=config.database) as session:
            await session.execute_write(cls._work_sequential, queries)


@dataclass
class _WorkloadQuery:
    query: str
    parameters: t.Optional[t.Dict[str, t.Any]]

    @classmethod
    def parse_multiple(cls, queries: t.Any) -> t.List[te.Self]:
        if not isinstance(queries, t.Iterable):
            raise TypeError("Workload queries must be a list")
        return [cls.parse(query) for query in queries]

    @classmethod
    def parse(cls, query: t.Any) -> te.Self:
        if not isinstance(query, dict):
            raise TypeError("Workload query must be a dict")
        if "text" not in query:
            raise ValueError("Workload query must have a text")
        text = query["text"]
        if not isinstance(text, str):
            raise TypeError("Workload query text must be a string")
        parameters = query.get("parameters")
        if parameters:
            if not isinstance(parameters, dict):
                raise TypeError("Workload query parameters must be a dict")
            if not all(isinstance(key, str) for key in parameters):
                raise TypeError(
                    "Workload query parameter keys must be strings"
                )
        return cls(text, parameters)


@dataclass
class _WorkloadConfig:
    database: t.Optional[str]
    routing: t.Literal["r", "w"]

    @classmethod
    def parse(cls, data: t.Any) -> te.Self:
        database = None
        if data.get("database", ""):
            database = data["database"]
            if not isinstance(database, str):
                raise TypeError("Workload database must be a string")

        routing: t.Literal["r", "w"] = "w"
        if "routing" in data:
            raw_routing = data["routing"]
            if not isinstance(routing, str):
                raise TypeError("Workload routing must be a string")
            if raw_routing == "read":
                routing = "r"
            elif raw_routing == "write":
                routing = "w"
            else:
                raise ValueError(
                    "Workload routing must be either 'read' or 'write'"
                )

        return cls(database, routing)

    def patch(self, data: t.Any) -> None:
        if "database" in data:
            self.database = data["database"]
        if "routing" in data:
            raw_routing = data["routing"]
            if raw_routing == "read":
                self.routing = "r"
            elif raw_routing == "write":
                self.routing = "w"
            else:
                raise ValueError(
                    "Workload routing must be either 'read' or 'write'"
                )


class _WorkloadMode(str, enum.Enum):
    SEQUENTIAL_SESSIONS = "sequentialSessions"
    SEQUENTIAL_TRANSACTIONS = "sequentialTransactions"
    SEQUENTIAL_QUERIES = "sequentialQueries"
    PARALLEL_SESSIONS = "parallelSessions"

    @classmethod
    def parse(cls, mode: t.Any) -> te.Self:
        if not isinstance(mode, str):
            raise TypeError("Workload mode must be a string")
        try:
            return cls(mode)
        except ValueError:
            raise ValueError(
                f"Unknown workload mode: {mode}. "
                f"Must be one of: {', '.join(cls.__members__)}"
            ) from None
