from datetime import datetime, timezone, timedelta

import numpy as np
import pytest
import xarray

from maridatadownloader import (BoxSubset, DownloaderCMEMS, DownloaderERA5, DownloaderETOPO, DownloaderGFS, Request,
                                TrajectorySubset, gfs, trajectories)


class TestGFS:

    def test_normalize(self, raw_gfs_like_dataset):
        result = DownloaderGFS().normalize(raw_gfs_like_dataset, Request())
        assert set(result.dims) == {'time', 'height_above_ground', 'latitude', 'longitude'}
        assert (np.diff(result.latitude) > 0).all()
        np.testing.assert_array_equal(result.longitude, [-180, -120, -60, 0, 60, 120])
        assert result.longitude.attrs == {'units': 'degrees_east'}
        # Data is moved together with the coordinates
        original = raw_gfs_like_dataset['Temperature_surface'].sel(lat=60, lon=300)
        np.testing.assert_array_equal(result['Temperature_surface'].sel(latitude=60, longitude=-60), original)

    def test_open_dataset_forecast_or_archive(self, monkeypatch):
        downloader = DownloaderGFS()
        opened = []
        monkeypatch.setattr(gfs, 'open_xarray_dataset', lambda url, chunks: opened.append(url) or xarray.Dataset())
        monkeypatch.setattr(downloader, '_open_archive', lambda *args: 'archive')

        now = datetime.now(timezone.utc)
        recent = BoxSubset(time=slice(now - timedelta(days=1), now))
        old = BoxSubset(time=slice(now - timedelta(days=10), now - timedelta(days=9)))
        downloader.open_dataset(Request.create(subset=recent))
        downloader.open_dataset(Request.create())
        assert opened == [DownloaderGFS.forecast_url]  # opened once, then cached
        assert downloader.open_dataset(Request.create(subset=old)) == 'archive'

    def test_open_archive(self, raw_gfs_like_dataset, monkeypatch):
        steps = iter(range(len(raw_gfs_like_dataset.time1)))
        monkeypatch.setattr(gfs, 'open_xarray_dataset',
                            lambda url, chunks: raw_gfs_like_dataset.isel(time1=[next(steps)]))
        start = datetime(2023, 5, 1, 1, tzinfo=timezone.utc)
        result = DownloaderGFS()._open_archive(start, start + timedelta(hours=5), ('Temperature_surface',))
        assert list(result.data_vars) == ['Temperature_surface']
        assert result.sizes['time'] == 3
        np.testing.assert_array_equal(result.time, raw_gfs_like_dataset.time1[:3])

    def test_urls_time_window(self):
        downloader = DownloaderGFS()
        urls = downloader._get_urls_time_window(datetime(2023, 5, 1, 1, tzinfo=timezone.utc),
                                                datetime(2023, 5, 1, 7, tzinfo=timezone.utc))
        assert [url.rsplit('/', 1)[1] for url in urls] == [
            'gfs.0p25.2023050100.f000.grib2', 'gfs.0p25.2023050100.f003.grib2',
            'gfs.0p25.2023050106.f000.grib2', 'gfs.0p25.2023050106.f003.grib2']
        with pytest.raises(ValueError):
            downloader._get_url(datetime(2023, 5, 1, 1, tzinfo=timezone.utc))


def test_etopo_normalize(dataset):
    raw = dataset.rename({'latitude': 'lat', 'longitude': 'lon'})
    result = DownloaderETOPO().normalize(raw, Request())
    assert {'latitude', 'longitude'} <= set(result.dims)


class TestERA5:

    def test_build_cds_request(self):
        subset = BoxSubset(time=slice('2024-01-31T22:30:00', '2024-02-01T01:00:00'), latitude=slice(50, 55),
                           longitude=slice(2, 8))
        request = Request.create(['10m_u_component_of_wind'], subset)
        cds_request = DownloaderERA5(api_key='key').build_cds_request(request)
        assert cds_request == {
            'product_type': ['reanalysis'],
            'variable': ['10m_u_component_of_wind'],
            'year': ['2024'],
            'month': ['01', '02'],
            'day': ['01', '31'],
            'time': ['00:00', '01:00', '22:00', '23:00'],
            'data_format': 'netcdf',
            'download_format': 'unarchived',
            'area': [55, 2, 50, 8],
        }

    def test_build_cds_request_trajectory(self):
        subset = TrajectorySubset(time=['2024-01-01T10:00', '2024-01-01T11:00'], latitude=[50, 51],
                                  longitude=[2, 3], buffer_deg=0.5)
        cds_request = DownloaderERA5(api_key='key').build_cds_request(Request.create('2m_temperature', subset))
        assert cds_request['area'] == [51.5, 1.5, 49.5, 3.5]
        assert cds_request['time'] == ['10:00', '11:00']

    def test_build_cds_request_errors(self):
        downloader = DownloaderERA5(api_key='key')
        with pytest.raises(ValueError, match='Parameters'):
            downloader.build_cds_request(Request.create(subset=BoxSubset(time='2024-01-01')))
        with pytest.raises(ValueError, match='time range'):
            downloader.build_cds_request(Request.create('2m_temperature'))

    def test_normalize(self, dataset):
        raw = dataset.rename({'time': 'valid_time'}).assign_coords(longitude=dataset.longitude + 355)
        raw = raw.isel(latitude=slice(None, None, -1))
        result = DownloaderERA5(api_key='key').normalize(raw, Request())
        assert 'time' in result.dims
        assert (np.diff(result.latitude) > 0).all()
        assert (np.diff(result.longitude) > 0).all()
        assert result.longitude.min() >= -180 and result.longitude.max() < 180

    def test_parameters_are_not_selected_again(self, dataset):
        request = Request.create('10m_u_component_of_wind')
        assert DownloaderERA5(api_key='key').select_parameters(dataset, request) is dataset


def test_cmems_open_dataset(dataset, monkeypatch):
    import copernicusmarine
    calls = []
    monkeypatch.setattr(copernicusmarine, 'open_dataset', lambda **kwargs: calls.append(kwargs) or dataset)
    downloader = DownloaderCMEMS('dataset-id', username='user', password='secret', dataset_version='202406')
    assert downloader.get_xarray_dataset('temperature', BoxSubset(latitude=51)).temperature.dims == (
        'time', 'longitude')
    downloader.get_xarray_dataset()
    assert calls == [{'dataset_id': 'dataset-id', 'username': 'user', 'password': 'secret',
                      'dataset_version': '202406'}]


def test_read_hf_data_positions(tmp_path):
    csv_file = tmp_path / 'positions.csv'
    csv_file.write_text('2023-09-20T09:00:00.000|5130.0,N,00245.0,W\n'
                        '2023-09-20T10:00:00.000|5136.0,S,00300.0,E\n')
    df = trajectories.read_hf_data_positions(csv_file)
    assert list(df.columns) == ['time', 'latitude', 'longitude']
    np.testing.assert_allclose(df['latitude'], [51.5, -51.6])
    np.testing.assert_allclose(df['longitude'], [-2.75, 3.0])
    assert df['time'][0] == datetime(2023, 9, 20, 9)
