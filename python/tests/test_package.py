"""The installed package and its native extension."""

import importlib.metadata
import importlib.resources

import rdlt


def test_the_package_and_its_extension_agree_on_the_version() -> None:
    assert rdlt.__version__ == importlib.metadata.version("rdlt")


def test_the_package_is_typed() -> None:
    assert importlib.resources.files("rdlt").joinpath("py.typed").is_file()
