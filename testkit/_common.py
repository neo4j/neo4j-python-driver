import os
import subprocess
import sys


TEST_BACKEND_VERSION = os.getenv("TEST_BACKEND_VERSION", "python")


def run(args, env=None):
    return subprocess.run(
        args, universal_newlines=True, stdout=sys.stdout, stderr=sys.stderr,
        check=True, env=env
    )


def run_python(args, env=None, warning_as_error=True):
    cmd = [TEST_BACKEND_VERSION, "-u"]
    if warning_as_error:
        cmd += ["-W", "error"]
    cmd += list(args)
    run(cmd, env=env)
