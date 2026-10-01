import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import pytest
import xarray

from conftest import TIMES, linear_field
from maridatadownloader.subsetting import BoxSubset, TrajectorySubset, _bracketing_slice, fill_nan, has_nan


class TestBoxSubset:

    def test_by_value_slices(self, dataset):
        subset = BoxSubset(time=slice('2024-01-01T03:00', '2024-01-01T09:00'), latitude=slice(51, 53),
                           longitude=slice(2, 3))
        result = subset.apply(dataset)
        assert dict(result.sizes) == {'time': 3, 'latitude': 3, 'longitude': 2}

    def test_by_index(self, dataset):
        result = BoxSubset(time=0, latitude=slice(0, 2), by='index').apply(dataset)
        assert dict(result.sizes) == {'latitude': 2, 'longitude': 8}
        assert result.time.values == TIMES[0]

    def test_inexact_match(self, dataset):
        result = BoxSubset(latitude=51.3, longitude=[2.2, 4.9], method='nearest').apply(dataset)
        assert result.latitude.item() == 51
        np.testing.assert_array_equal(result.longitude, [2, 5])

    def test_interpolate(self, dataset):
        result = BoxSubset(time=TIMES[1], latitude=51.5, longitude=2.25, interpolate=True).apply(dataset)
        assert result.temperature.item() == pytest.approx(3 + 515 + 225)

    def test_timezone_aware_time(self, dataset):
        start = datetime(2024, 1, 1, 4, tzinfo=timezone(timedelta(hours=1)))  # 03:00 UTC
        end = datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
        result = BoxSubset(time=slice(start, end)).apply(dataset)
        np.testing.assert_array_equal(result.time, TIMES[1:3])

    def test_ignores_non_dimension_indexers(self, dataset, caplog):
        with caplog.at_level(logging.WARNING):
            result = BoxSubset(latitude=50, extra={'depth': 0}).apply(dataset)
        assert 'depth' in caplog.text
        assert 'latitude' not in result.dims

    def test_invalid_arguments(self):
        with pytest.raises(ValueError):
            BoxSubset(by='label')
        with pytest.raises(ValueError):
            BoxSubset(latitude=0, by='index', interpolate=True)

    def test_time_bounds(self):
        assert BoxSubset().time_bounds() is None
        assert BoxSubset(time=slice(None, '2024-01-01')).time_bounds() is None
        assert BoxSubset(time=0, by='index').time_bounds() is None
        start, end = BoxSubset(time=slice('2024-01-02 06:00:00', datetime(2024, 1, 1))).time_bounds()
        assert start == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert end == datetime(2024, 1, 2, 6, tzinfo=timezone.utc)

    def test_bbox(self):
        assert BoxSubset(latitude=slice(55, 50), longitude=[3, 1, 2]).bbox() == (1, 50, 3, 55)
        assert BoxSubset(latitude=52.0, longitude=slice(1, 2)).bbox() == (1, 52, 2, 52)
        assert BoxSubset(latitude=52.0).bbox() is None
        assert BoxSubset(latitude=0, longitude=0, by='index').bbox() is None


class TestTrajectorySubset:

    times = [datetime(2024, 1, 1, 1, 45), datetime(2024, 1, 1, 4), datetime(2024, 1, 1, 7, 15)]
    latitudes = [51.25, 52.4, 53.75]
    longitudes = [1.6, 2.0, 4.25]

    def make_subset(self, **kwargs):
        return TrajectorySubset(time=self.times, latitude=self.latitudes, longitude=self.longitudes, **kwargs)

    def test_linear_interpolation(self, dataset):
        result = self.make_subset(method='linear').apply(dataset)
        expected = [hours + 10 * lat + 100 * lon
                    for hours, lat, lon in zip([1.75, 4, 7.25], self.latitudes, self.longitudes)]
        assert result.temperature.dims == ('trajectory',)
        np.testing.assert_allclose(result.temperature, expected)
        np.testing.assert_allclose(result.latitude, self.latitudes)

    def test_nearest_interpolation(self, dataset):
        result = self.make_subset(method='nearest').apply(dataset)
        expected = linear_field(pd.DatetimeIndex(['2024-01-01T03:00', '2024-01-01T03:00', '2024-01-01T06:00']),
                                np.array([51.0, 52.0, 54.0]), np.array([2.0, 2.0, 4.0]))
        np.testing.assert_allclose(result.temperature, [expected[i, i, i] for i in range(3)])
        # interp keeps the coordinates of the trajectory
        np.testing.assert_allclose(result.longitude, self.longitudes)

    def test_inexact_match(self, dataset):
        result = self.make_subset(interpolate=False).apply(dataset)
        # sel returns the coordinates of the selected grid points
        np.testing.assert_allclose(result.longitude, [2.0, 2.0, 4.0])

    def test_extra_indexers(self, dataset):
        dataset = dataset.expand_dims(height_above_ground=[10.0, 80.0])
        result = self.make_subset(method='linear', extra={'height_above_ground': 10.0}).apply(dataset)
        assert result.temperature.dims == ('trajectory',)

    def test_fill_nan(self, dataset):
        dataset = dataset.copy(deep=True)
        dataset['temperature'][:, 1:3, 1:3] = np.nan
        assert has_nan(self.make_subset(method='nearest').apply(dataset))
        result = self.make_subset(method='nearest', fill_nan='linear').apply(dataset)
        assert not has_nan(result)

    def test_dataset_without_time(self, dataset):
        result = self.make_subset(method='linear').apply(dataset.isel(time=0))
        assert result.temperature.dims == ('trajectory',)

    def test_time_conversion(self):
        subset = TrajectorySubset(time=['2024-01-01T01:00:00', datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
                                        np.datetime64('2024-01-01T05:00')],
                                  latitude=[1, 2, 3], longitude=[4, 5, 6])
        assert subset.time_bounds() == (datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
                                        datetime(2024, 1, 1, 5, tzinfo=timezone.utc))

    def test_bbox_includes_buffer(self):
        assert self.make_subset(buffer_deg=0.5).bbox() == pytest.approx((1.1, 50.75, 4.75, 54.25))

    def test_length_mismatch(self):
        with pytest.raises(ValueError):
            TrajectorySubset(time=self.times, latitude=[1, 2], longitude=[1, 2])

    def test_from_dataframe(self):
        df = pd.DataFrame({'time': self.times, 'latitude': self.latitudes, 'longitude': self.longitudes})
        subset = TrajectorySubset.from_dataframe(df, every_nth_row=2, method='linear')
        np.testing.assert_allclose(subset.latitude, [51.25, 53.75])
        assert subset.method == 'linear'


@pytest.mark.parametrize('values', [np.arange(10.0), np.arange(10.0)[::-1]])
@pytest.mark.parametrize('value_range, expected', [
    ((2.5, 4.5), [2, 3, 4, 5]),
    ((3.0, 4.0), [3, 4]),
    ((-5, 1.5), [0, 1, 2]),
    ((8.5, 20), [8, 9]),
])
def test_bracketing_slice(values, value_range, expected):
    coord = xarray.DataArray(values)
    result = coord[_bracketing_slice(coord, *value_range)]
    assert sorted(result.values) == expected


def test_fill_nan_extrapolates_at_edges(dataset):
    data = dataset.temperature.isel(time=0).copy()
    data[0, :] = np.nan
    filled = fill_nan(data)
    np.testing.assert_allclose(filled, dataset.temperature.isel(time=0))
