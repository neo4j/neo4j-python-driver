import subprocess


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    run([
        "python", "-m", "tox", "-c", "tox-unit.ini"])
