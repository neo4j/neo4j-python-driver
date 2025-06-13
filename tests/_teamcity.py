# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import os
import re
import time
import typing as t


if t.TYPE_CHECKING:
    import pytest


__all__ = [
    "pytest_collection_finish",
    "pytest_runtest_logreport",
    "pytest_sessionstart",
    "pytest_unconfigure",
]


_ENABLED = os.environ.get("TEST_IN_TEAMCITY", "").upper() in {
    "TRUE",
    "1",
    "Y",
    "YES",
    "ON",
}


_SUITE_NAME = os.environ.get("TEST_SUITE_NAME")


def _escape(s: object) -> str:
    s = str(s)
    s = s.replace("|", "||")
    s = s.replace("\n", "|n")
    s = s.replace("\r", "|r")
    s = s.replace("'", "|'")
    s = s.replace("[", "|[")
    s = s.replace("]", "|]")
    return s  # noqa: RET504 - subjectively easier to read this way


def _message(title: str, **entries: object) -> None:
    if "timestamp" not in entries:
        now = time.time()
        now_s, now_sub_s = divmod(now, 1)
        now_tuple = time.localtime(now_s)
        entries["timestamp"] = (
            time.strftime("%Y-%m-%dT%H:%M:%S", now_tuple)
            + f".{int(now_sub_s * 1000):03}"
        )
    str_entries = " ".join(f"{k}='{_escape(v)}'" for k, v in entries.items())
    if str_entries:
        str_entries = " " + str_entries

    print(f"\n##teamcity[{title}{str_entries}]", flush=True)  # noqa: T201
    # [noqa] to allow print as that's the whole purpose of this
    # make-shift pytest plugin


def pytest_sessionstart(session: pytest.Session) -> None:
    if not (_ENABLED and _SUITE_NAME):
        return
    _message("testSuiteStarted", name=_SUITE_NAME)


def pytest_unconfigure(config: pytest.Config) -> None:
    if not (_ENABLED and _SUITE_NAME):
        return
    _message("testSuiteFinished", name=_SUITE_NAME)


def pytest_collection_finish(session: pytest.Session) -> None:
    if not _ENABLED:
        return
    _message("testCount", count=len(session.items))


# function taken from teamcity-messages package
# Copyright JetBrains, licensed under Apache 2.0
# changes applied:
# - non-functional changes (e.g., formatting, removed dead code)
# - removed support for pep8-check and pylint
# - simplified code using str.removesuffix
def format_test_id(nodeid: str) -> str:
    test_id = nodeid

    if test_id:
        if test_id.find("::") < 0:
            test_id += "::top_level"
    else:
        test_id = "top_level"

    first_bracket = test_id.find("[")
    if first_bracket > 0:
        # [] -> (), make it look like nose parameterized tests
        params = "(" + test_id[first_bracket + 1 :]
        if params.endswith("]"):
            params = params[:-1] + ")"
        test_id = test_id[:first_bracket]
        test_id = test_id.removesuffix("::")
    else:
        params = ""

    test_id = test_id.replace("::()::", "::")
    test_id = re.sub(r"\.pyc?::", r"::", test_id)
    test_id = test_id.replace(".", "_")
    test_id = test_id.replace(os.sep, ".")
    test_id = test_id.replace("/", ".")
    test_id = test_id.replace("::", ".")

    if params:
        params = params.replace(".", "_")
        test_id += params

    return test_id


def _report_output(test_id: str, stdout: str, stderr: str) -> None:
    block_name = None
    if stdout or stderr:
        block_name = f"{test_id} output"
        _message("blockOpened", name=block_name)
    if stdout:
        _message("testStdOut", name=test_id, out=stdout)
    if stderr:
        _message("testStdErr", name=test_id, out=stderr)
    if block_name:
        _message("blockClosed", name=block_name)


def _skip_reason(report: pytest.TestReport) -> str | None:
    if isinstance(report.longrepr, tuple):
        return report.longrepr[2]
    if isinstance(report.longrepr, str):
        return report.longrepr
    return None


def _report_skip(test_id: str, reason: str | None) -> None:
    if reason is None:
        _message("testIgnored", name=test_id)
    else:
        _message("testIgnored", name=test_id, message=reason)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if not _ENABLED:
        return

    test_id = format_test_id(report.nodeid)

    test_stdouts = []
    test_stderrs = []
    for section_name, section_data in report.sections:
        if not section_data:
            continue
        if "stdout" in section_name:
            test_stdouts.append(
                f"===== [{section_name}] =====\n{section_data}"
            )
        if "stderr" in section_name:
            test_stderrs.append(
                f"===== [{section_name}] =====\n{section_data}"
            )
    test_stdout = "\n".join(test_stdouts)
    test_stderr = "\n".join(test_stderrs)

    if report.when == "teardown":
        _report_output(test_id, test_stdout, test_stderr)
        test_duration_ms = int(report.duration * 1000)
        _message("testFinished", name=test_id, duration=test_duration_ms)
        if report.outcome == "skipped":
            # a little late to skip the test, eh?
            test_stage_id = f"{test_id}___teardown"
            _report_skip(test_stage_id, _skip_reason(report))

    if report.when in {"setup", "teardown"} and report.outcome == "failed":
        test_stage_id = f"{test_id}__{report.when}"
        _message("testStarted", name=test_stage_id)
        _report_output(test_stage_id, test_stdout, test_stderr)
        _message(
            "testFailed",
            name=test_stage_id,
            message=f"{report.when.capitalize()} failed",
            details=report.longreprtext,
        )
        _message("testFinished", name=test_stage_id)

    if report.when == "setup":
        _message("testStarted", name=test_id)
        if report.outcome == "skipped":
            _report_skip(test_id, _skip_reason(report))

    if report.when == "call":
        if report.outcome == "failed":
            _message("testFailed", name=test_id, message=report.longreprtext)
        elif report.outcome == "skipped":
            _report_skip(test_id, _skip_reason(report))
