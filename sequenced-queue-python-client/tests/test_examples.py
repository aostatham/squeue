import py_compile
from pathlib import Path


def test_python_examples_compile():
    repo_root = Path(__file__).resolve().parents[2]

    py_compile.compile(repo_root / "examples/python-producer/producer.py", doraise=True)
    py_compile.compile(repo_root / "examples/python-worker/worker.py", doraise=True)
