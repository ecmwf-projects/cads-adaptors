import os
import pathlib

import cads_adaptors


def test_cache_tmp_path_dummy_adaptor(tmp_path: pathlib.Path) -> None:
    dummy_adaptor = cads_adaptors.DummyAdaptor(None, cache_tmp_path=tmp_path)
    result = dummy_adaptor.retrieve({"size": 1})
    assert result.name == str(tmp_path / "dummy.grib")
    assert os.path.getsize(result.name) == 1
