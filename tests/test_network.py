"""Smoke tests against the remote data sources. Run with `pytest -m network`."""
from datetime import datetime, timezone, timedelta

import pytest

from maridatadownloader import BoxSubset, TrajectorySubset, get_downloader

pytestmark = pytest.mark.network


def test_etopo_box(tmp_path):
    with get_downloader('etopo') as etopo:
        subset = BoxSubset(latitude=slice(54.0, 54.1), longitude=slice(7.0, 7.1))
        path = etopo.save_to_file(tmp_path / 'etopo.nc', subset=subset)
        dataset = etopo.get_xarray_dataset(subset=subset)
    assert path.exists()
    assert dataset.sizes['latitude'] > 0 and dataset.sizes['longitude'] > 0


def test_gfs_forecast_trajectory():
    now = datetime.now(timezone.utc)
    subset = TrajectorySubset(time=[now, now + timedelta(hours=3)], latitude=[54.0, 54.5], longitude=[7.0, -7.0],
                              method='linear', extra={'height_above_ground': 10})
    with get_downloader('gfs') as gfs:
        dataset = gfs.get_xarray_dataset(['Temperature_surface', 'u-component_of_wind_height_above_ground'],
                                         subset).load()
    assert dataset['Temperature_surface'].dims == ('trajectory',)
    assert dataset['u-component_of_wind_height_above_ground'].dims == ('trajectory',)
    assert not dataset['Temperature_surface'].isnull().any()


def test_gfs_archive_box():
    subset = BoxSubset(time=slice('2023-05-01T00:00:00', '2023-05-01T03:00:00'), latitude=slice(54, 55),
                       longitude=slice(-1, 1))
    with get_downloader('gfs') as gfs:
        dataset = gfs.get_xarray_dataset('Temperature_surface', subset).load()
    assert dataset.sizes['time'] == 2
    assert dataset.longitude.min() < 0 < dataset.longitude.max()
