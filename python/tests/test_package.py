"""The installed package and its native extension."""

import importlib.metadata
import importlib.resources
import subprocess
import sys

import pytest

import rdlt


def test_the_package_and_its_extension_agree_on_the_version() -> None:
    assert rdlt.__version__ == importlib.metadata.version("rdlt")


def test_the_package_is_typed() -> None:
    assert importlib.resources.files("rdlt").joinpath("py.typed").is_file()


@pytest.mark.parametrize("module", ["rdlt", "rdlt.connector", "rdlt.testing"])
def test_importing_the_package_does_not_load_pyarrow(module: str) -> None:
    script = f"import sys, {module}; sys.exit(1 if 'pyarrow' in sys.modules else 0)"
    done = subprocess.run([sys.executable, "-c", script], check=False, timeout=60)
    assert done.returncode == 0
