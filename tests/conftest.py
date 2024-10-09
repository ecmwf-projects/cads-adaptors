import pathlib
from collections.abc import Generator

import cacholote
import pytest


@pytest.fixture(autouse=True)
def set_cache(
    tmp_path: pathlib.Path,
) -> Generator[None, None, None]:
    with cacholote.config.set(
        cache_db_urlpath="sqlite:///" + str(tmp_path / "cacholote.db"),
        cache_files_urlpath=str(tmp_path / "cache_files"),
    ):
        yield
