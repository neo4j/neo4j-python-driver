import subprocess
import os
import sys


if __name__ == "__main__":
    # Until below works
    sys.exit(0)
    cmd = ["python", "-m", "tox", "-c", "tox-performance.ini"]
    uri = "%s://%s:%s" % (
            os.environ["TEST_NEO4J_SCHEME"],
            os.environ["TEST_NEO4J_HOST"],
            os.environ["TEST_NEO4J_PORT"])
    env = {
            "NEO4J_USER": os.environ["TEST_NEO4J_USER"],
            "NEO4J_PASSWORD": os.environ["TEST_NEO4J_PASS"],
            "NEO4J_URI": uri}
    subprocess.check_call(cmd, universal_newlines=True,
                          stderr=subprocess.STDOUT, env=env)
