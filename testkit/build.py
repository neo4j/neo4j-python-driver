"""
Executed in driver container.
Responsible for building driver and test backend.
"""


import subprocess
import sys


def run(args, env=None):
    subprocess.run(args, universal_newlines=True, stdout=sys.stdout,
                   stderr=sys.stderr, check=True, env=env)


if __name__ == "__main__":
    run(["python", "setup.py", "build"])
    run(["python", "-m", "pip", "install", "-U", "pip"])
    run(["python", "-m", "pip", "install", "-Ur",
         "testkitbackend/requirements.txt"])
