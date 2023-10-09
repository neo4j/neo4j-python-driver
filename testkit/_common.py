import os
import re
import subprocess
import sys


TEST_BACKEND_VERSION = os.getenv("TEST_BACKEND_VERSION", "python")


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
    if get_python_version() >= (3, 12):
        # Ignore warnings for Python 3.12 for now
        # https://github.com/dateutil/dateutil/issues/1284 needs to be released
        # and propagate through our dependency graph
        warning_as_error = False
    if warning_as_error:
        cmd += ["-W", "error"]
    cmd += list(args)
    run(cmd, env=env)
