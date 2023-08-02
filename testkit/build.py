"""
Executed in driver container.
Responsible for building driver and test backend.
"""


from _common import run_python


if __name__ == "__main__":
    run_python(["setup.py", "build"])
    run_python(["-m", "pip", "install", "-U", "pip"], warning_as_error=False)
    run_python(
        ["-m", "pip", "install", "-Ur", "testkitbackend/requirements.txt"],
        warning_as_error=False
    )
