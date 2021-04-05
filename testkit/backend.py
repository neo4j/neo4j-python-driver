import subprocess

if __name__ == "__main__":
    err = open("/artifacts/backenderr.log", "w")
    out = open("/artifacts/backendout.log", "w")
    subprocess.check_call(
        ["python", "-m", "testkitbackend"], stdout=out, stderr=err)
