import subprocess
import sys


def run(args):
    subprocess.run(
        args, universal_newlines=True, stdout=sys.stdout, stderr=sys.stderr,
        check=True
    )


if __name__ == "__main__":
    run([
        "python", "-m", "tox", "-c", "tox-unit.ini"])
