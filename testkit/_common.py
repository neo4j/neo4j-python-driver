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


import os
import re
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path


TEST_BACKEND_VERSION = os.getenv("TEST_BACKEND_VERSION", "python")
TEST_BACKEND_EXTENSIONS = (
    os.getenv("TEST_BACKEND_EXTENSIONS", "").lower()
    in ("yes", "y", "true", "t", "1", "on")
)


_EXT_BUILD_ORIGINAL = None
_EXT_BUILD_RE = re.compile(
    r"^(native_extensions_build\s*=\s*)(.*)(  # \[script-set\])$"
)
_EXT_FORCE_ORIGINAL = None
_EXT_FORCE_RE = re.compile(
    r"^(native_extensions_force\s*=\s*)(.*)(  # \[script-set\])$"
)


def _write_extension_config(build, force, check=False, store_original=False):
    global _EXT_BUILD_ORIGINAL
    global _EXT_FORCE_ORIGINAL

    meta_path = Path(__file__).parents[1] / "src" / "neo4j" / "_meta.py"
    repl_count_build = repl_count_force = 0
    with (meta_path.open("r+") as fd):
        lines = fd.readlines()
        for i in range(len(lines)):
            if store_original:
                any_match = False
                match = _EXT_BUILD_RE.match(lines[i])
                if match:
                    any_match = True
                    if not match.group(2) in ("True", "False"):
                        raise RuntimeError(
                            "Could not determine extensions build flag."
                        )
                    _EXT_BUILD_ORIGINAL = match.group(2) == "True"

                match = _EXT_FORCE_RE.match(lines[i])
                if match:
                    any_match = True
                    if match.group(2) == "True":
                        _EXT_FORCE_ORIGINAL = True
                    elif match.group(2) == "False":
                        _EXT_FORCE_ORIGINAL = False
                    else:
                        raise RuntimeError(
                            "Could not determine extensions force flag."
                        )
                if not any_match:
                    continue  # nothing to replace here
            lines[i], repl_count = _EXT_BUILD_RE.subn(rf"\1{build}\3",
                                                      lines[i])
            repl_count_build += repl_count
            lines[i], repl_count = _EXT_FORCE_RE.subn(rf"\1{force}\3",
                                                      lines[i])
            repl_count_force += repl_count
        if check and (repl_count_build != 1 or repl_count_force != 1):
            raise RuntimeError(
                "Could not set native extensions build flags. "
                f"Found build: {repl_count_build}, force: {repl_count_force}."
            )
        fd.seek(0)
        fd.writelines(lines)
        fd.truncate()


def configure_extensions(available):
    _write_extension_config(available, available,
                            check=True, store_original=True)


def restore_extensions_config():
    if _EXT_BUILD_ORIGINAL is None or _EXT_FORCE_ORIGINAL is None:
        raise RuntimeError("Could not restore extensions config: not stored.")
    _write_extension_config(_EXT_BUILD_ORIGINAL, _EXT_FORCE_ORIGINAL)


@contextmanager
def configured_extensions():
    configure_extensions(TEST_BACKEND_EXTENSIONS)
    try:
        yield
    finally:
        restore_extensions_config()


def run(args, env=None):
    print(args)
    return subprocess.run(
        args, universal_newlines=True, stdout=sys.stdout, stderr=sys.stderr,
        check=True, env=env
    )


def get_python_version():
    cmd = [TEST_BACKEND_VERSION, "-V"]
    res = subprocess.check_output(cmd, universal_newlines=True,
                                  stderr=sys.stderr)
    raw_version = re.match(r"(?:.*?)((?:\d+\.)+(?:\d+))", res).group(1)
    return tuple(int(e) for e in raw_version.split("."))


def run_python(args, env=None, warning_as_error=True):
    cmd = [TEST_BACKEND_VERSION, "-u"]
    if warning_as_error:
        cmd += ["-W", "error"]
    cmd += list(args)
    run(cmd, env=env)
