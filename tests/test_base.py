import pytest
import xarray

from maridatadownloader import BoxSubset, Downloader, Request, get_downloader, register
from maridatadownloader.registry import available_downloaders
from maridatadownloader.utils import rename_if_present


@register("dummy")
class DummyDownloader(Downloader):
    """Serves an in-memory dataset with non-canonical names and records the hook calls"""

    def __init__(self, source):
        self.source = source
        self.calls = []

    def open_dataset(self, request):
        self.calls.append("open_dataset")
        self._dataset = self.source
        return self._dataset

    def select_parameters(self, dataset, request):
        self.calls.append("select_parameters")
        return super().select_parameters(dataset, request)

    def normalize(self, dataset, request):
        self.calls.append("normalize")
        return rename_if_present(
            dataset, {"lat": "latitude", "lon": "longitude", "time1": "time"}
        )

    def postprocess(self, dataset, request):
        self.calls.append("postprocess")
        return dataset.assign_attrs(postprocessed=1)


@pytest.fixture
def source(dataset):
    return dataset.rename(
        {"latitude": "lat", "longitude": "lon", "time": "time1"}
    ).assign_attrs(_NCProperties="version=2,netcdf=4.9.2")


def test_pipeline(source):
    downloader = DummyDownloader(source)
    # The subset uses canonical names, so it only works if normalize is applied before subsetting
    result = downloader.get_xarray_dataset(
        "temperature", BoxSubset(latitude=slice(51, 52))
    )
    assert downloader.calls == [
        "open_dataset",
        "select_parameters",
        "normalize",
        "postprocess",
    ]
    assert list(result.data_vars) == ["temperature"]
    assert dict(result.sizes) == {"time": 5, "latitude": 2, "longitude": 8}
    assert result.attrs["postprocessed"] == 1


def test_without_parameters_and_subset(source):
    result = DummyDownloader(source).get_xarray_dataset()
    assert set(result.data_vars) == {"temperature", "pressure"}
    assert dict(result.sizes) == {"time": 5, "latitude": 6, "longitude": 8}


def test_request_create():
    assert Request.create("a").parameters == ("a",)
    assert Request.create(["a", "b"]).parameters == ("a", "b")
    assert Request.create().parameters == ()
    assert Request.create().time_bounds() is None
    assert Request.create().bbox() is None
    assert Request.create(subset=BoxSubset(latitude=1, longitude=2)).bbox() == (
        2,
        1,
        2,
        1,
    )


def test_save_to_file(source, tmp_path):
    path = DummyDownloader(source).save_to_file(
        tmp_path / "out.nc", ["temperature"], BoxSubset(latitude=slice(51, 52))
    )
    with xarray.open_dataset(path) as saved:
        assert list(saved.data_vars) == ["temperature"]
        assert dict(saved.sizes) == {"time": 5, "latitude": 2, "longitude": 8}
    # The reserved attribute is only removed from the written copy
    assert "_NCProperties" in source.attrs


def test_close_and_context_manager(source):
    with DummyDownloader(source) as downloader:
        downloader.get_xarray_dataset()
        assert downloader._dataset is not None
    assert downloader._dataset is None


def test_registry(source):
    assert "dummy" in available_downloaders()
    downloader = get_downloader("DUMMY", source=source)
    assert isinstance(downloader, DummyDownloader)
    assert downloader.name == "dummy"
    with pytest.raises(ValueError, match="Available"):
        get_downloader("unknown")
    with pytest.raises(ValueError, match="already registered"):
        register("dummy")(type("Other", (DummyDownloader,), {}))
