"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""
import subprocess


def run(args, env=None):
    subprocess.run(args, universal_newlines=True, stderr=subprocess.STDOUT,
                   check=True, env=env)


if __name__ == "__main__":
    run(["python", "setup.py", "build"])
