import numpy as np
import pandas as pd
import pytest
import xarray

TIMES = pd.date_range('2024-01-01', periods=5, freq='3h')
LATITUDES = np.arange(50.0, 56.0, 1.0)
LONGITUDES = np.arange(0.0, 8.0, 1.0)


def linear_field(times, latitudes, longitudes):
    """Values which are linear in time (hours), latitude and longitude, so linear interpolation is exact"""
    hours = (times - TIMES[0]) / pd.Timedelta(hours=1)
    return (np.asarray(hours)[:, None, None] + 10 * latitudes[None, :, None] + 100 * longitudes[None, None, :])


@pytest.fixture
def dataset():
    """Dataset following the package conventions"""
    return xarray.Dataset(
        {
            'temperature': (('time', 'latitude', 'longitude'), linear_field(TIMES, LATITUDES, LONGITUDES)),
            'pressure': (('time', 'latitude', 'longitude'), np.ones((len(TIMES), len(LATITUDES), len(LONGITUDES)))),
        },
        coords={'time': TIMES, 'latitude': LATITUDES, 'longitude': LONGITUDES},
    )


@pytest.fixture
def raw_gfs_like_dataset():
    """Dataset with GFS-like conventions: descending latitude, longitude 0-360, numbered coordinates"""
    latitudes = np.arange(90.0, -91.0, -30.0)
    longitudes = np.arange(0.0, 360.0, 60.0)
    shape = (len(TIMES), 2, len(latitudes), len(longitudes))
    lon = xarray.Variable('lon', longitudes, attrs={'units': 'degrees_east'})
    return xarray.Dataset(
        {
            'u-component_of_wind': (('time1', 'height_above_ground2', 'lat', 'lon'), np.random.rand(*shape)),
            'Temperature_surface': (('time1', 'lat', 'lon'), np.random.rand(*shape[:1] + shape[2:])),
        },
        coords={'time1': TIMES, 'height_above_ground2': [10.0, 80.0], 'lat': latitudes, 'lon': lon},
        attrs={'_NCProperties': 'version=2,netcdf=4.9.2'},
    )
