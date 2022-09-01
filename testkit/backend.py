import subprocess
import sys

if __name__ == "__main__":
    subprocess.check_call(
        ["python", "-W", "error", "-m", "testkitbackend"],
        stdout=sys.stdout, stderr=sys.stderr
    )
