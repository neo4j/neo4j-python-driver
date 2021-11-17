import subprocess
import sys

if __name__ == "__main__":
    subprocess.check_call(
        ["python", "-m", "testkitbackend"],
        stdout=sys.stdout, stderr=sys.stderr
    )
