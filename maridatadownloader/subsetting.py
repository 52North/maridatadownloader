"""
Subsetting strategies which are applied to an xarray.Dataset with canonical coordinate names
('time', 'latitude', 'longitude', ...).

References:
 - https://docs.xarray.dev/en/stable/user-guide/indexing.html
 - https://docs.xarray.dev/en/stable/user-guide/indexing.html#vectorized-indexing
 - https://docs.xarray.dev/en/stable/user-guide/interpolation.html#advanced-interpolation
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import xarray

from maridatadownloader.utils import time_range, to_naive_utc, to_utc_index

logger = logging.getLogger(__name__)


class Subset(ABC):
    """Base class of all subsetting strategies"""

    @abstractmethod
    def apply(self, dataset: xarray.Dataset) -> xarray.Dataset:
        """Return the subset of the dataset"""

    def time_bounds(self) -> tuple[datetime, datetime] | None:
        """Return the requested time range as timezone-aware (UTC) datetime objects or None if unknown"""
        return None

    def bbox(self) -> tuple[float, float, float, float] | None:
        """Return the requested bounding box as (lon_min, lat_min, lon_max, lat_max) or None if unknown"""
        return None


@dataclass
class BoxSubset(Subset):
    """
    Orthogonal subsetting (data cube).

    Each coordinate can be given as a scalar, a sequence or a slice. Coordinates which are not dimensions of the
    dataset are ignored (with a warning).

    :param by: 'value' uses xarray.Dataset.sel, 'index' uses xarray.Dataset.isel
    :param method: For by='value', the method for inexact matches (e.g. 'nearest'). If interpolate=True, the
        interpolation method (default: 'linear').
    :param interpolate: Set to True to interpolate to off-grid values using xarray.Dataset.interp.
        Slices are always applied with xarray.Dataset.sel.
    :param extra: Indexers for additional coordinates, e.g. {'depth': 0.494, 'height_above_ground': 10}
    :param options: Additional keyword arguments passed to the xarray method (e.g. tolerance)
    """
    time: Any = None
    latitude: Any = None
    longitude: Any = None
    extra: dict = field(default_factory=dict)
    by: str = 'value'
    method: str | None = None
    interpolate: bool = False
    options: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.by not in ('value', 'index'):
            raise ValueError(f"'by' must be 'value' or 'index', got '{self.by}'")
        if self.interpolate and self.by == 'index':
            raise ValueError("Interpolation cannot be applied with index subsetting")

    @property
    def indexers(self) -> dict:
        indexers = {'time': self.time, 'latitude': self.latitude, 'longitude': self.longitude, **self.extra}
        return {name: value for name, value in indexers.items() if value is not None}

    def apply(self, dataset):
        indexers = _dimension_indexers(dataset, self.indexers)
        if self.by == 'index':
            return dataset.isel(indexers, **self.options)

        if 'time' in indexers:
            indexers['time'] = to_naive_utc(indexers['time'])
        # xarray doesn't support inexact matches or interpolation for slices, so apply them separately
        slices = {name: value for name, value in indexers.items() if isinstance(value, slice)}
        points = {name: value for name, value in indexers.items() if not isinstance(value, slice)}
        if slices:
            dataset = dataset.sel(slices)
        if points:
            if self.interpolate:
                dataset = dataset.interp(points, method=self.method or 'linear', **self.options)
            else:
                dataset = dataset.sel(points, method=self.method, **self.options)
        return dataset

    def time_bounds(self):
        if self.time is None or self.by == 'index':
            return None
        return time_range(self.time)

    def bbox(self):
        if self.by == 'index':
            return None
        lon_range = _value_range(self.longitude)
        lat_range = _value_range(self.latitude)
        if lon_range is None or lat_range is None:
            return None
        return lon_range[0], lat_range[0], lon_range[1], lat_range[1]


@dataclass
class TrajectorySubset(Subset):
    """
    Subsetting along a space-time trajectory (vectorized indexing along the dimension `dim`).

    Before the actual subsetting, the dataset is reduced to a sub cube which covers the trajectory including the
    next grid points beyond its bounds and a spatial buffer. Optionally, NaN values in the sub cube are filled
    (e.g. for ocean data with NaN values on land pixels) to avoid NaN values along trajectories close to coasts.

    :param time: sequence of times (timezone-naive values are interpreted as UTC)
    :param latitude: sequence of latitudes
    :param longitude: sequence of longitudes
    :param method: Interpolation method ('linear' or 'nearest') or, if interpolate=False, the method for inexact
        matches
    :param interpolate: If True (default), use xarray.Dataset.interp so the result has the coordinates of the
        trajectory. If False, use xarray.Dataset.sel (inexact matches) so the result has the coordinates of the
        selected grid points.
    :param buffer_deg: spatial buffer in degrees added to the sub cube
    :param fill_nan: Method used to fill NaN values in the sub cube via extrapolation (e.g. 'linear' or
        'nearest'). No filling is applied if None.
    :param extra: Indexers for additional coordinates, e.g. {'height_above_ground': 10}. These are applied as
        exact matches.
    :param dim: name of the trajectory dimension
    :param options: Additional keyword arguments passed to xarray.Dataset.interp or xarray.Dataset.sel
    """
    time: Any
    latitude: Any
    longitude: Any
    method: str = 'nearest'
    interpolate: bool = True
    buffer_deg: float = 1.0
    fill_nan: str | None = None
    extra: dict = field(default_factory=dict)
    dim: str = 'trajectory'
    options: dict = field(default_factory=dict)

    def __post_init__(self):
        self.time = to_utc_index(self.time)
        self.latitude = np.atleast_1d(np.asarray(self.latitude, dtype=float))
        self.longitude = np.atleast_1d(np.asarray(self.longitude, dtype=float))
        if not len(self.time) == len(self.latitude) == len(self.longitude):
            raise ValueError("time, latitude and longitude must have the same length")

    @classmethod
    def from_dataframe(cls, dataframe, every_nth_row=1, **kwargs):
        """Create a TrajectorySubset from a pandas.DataFrame with the columns 'time', 'latitude' and 'longitude'"""
        rows = dataframe.iloc[::every_nth_row]
        return cls(time=rows['time'], latitude=rows['latitude'], longitude=rows['longitude'], **kwargs)

    def apply(self, dataset):
        times = self.time.tz_convert(None).values
        lon_min, lat_min, lon_max, lat_max = self.bbox()
        sub_cube_ranges = {'time': (times.min(), times.max()),
                           'latitude': (lat_min, lat_max),
                           'longitude': (lon_min, lon_max)}
        sub_cube = {name: _bracketing_slice(dataset[name], *value_range)
                    for name, value_range in sub_cube_ranges.items() if name in dataset.dims}
        dataset = dataset.isel(sub_cube)

        extra = _dimension_indexers(dataset, self.extra)
        if extra:
            dataset = dataset.sel(extra)

        if self.fill_nan and has_nan(dataset):
            dataset = fill_nan(dataset.load(), method=self.fill_nan)

        points = {name: xarray.DataArray(values, dims=self.dim)
                  for name, values in (('time', times), ('latitude', self.latitude), ('longitude', self.longitude))
                  if name in dataset.dims}
        if self.interpolate:
            return dataset.interp(points, method=self.method, **self.options)
        return dataset.sel(points, method=self.method, **self.options)

    def time_bounds(self):
        return self.time.min().to_pydatetime(), self.time.max().to_pydatetime()

    def bbox(self):
        return (self.longitude.min() - self.buffer_deg, self.latitude.min() - self.buffer_deg,
                self.longitude.max() + self.buffer_deg, self.latitude.max() + self.buffer_deg)


def fill_nan(dataset, **kwargs):
    """
    Fill NaN values using extrapolation along longitude and latitude (in this order; changing the order changes the
    result).

    :param dataset: xarray.Dataset or xarray.DataArray
    :param kwargs: Extra keyword arguments passed to interpolate_na, e.g. method.
                   Note that 'limit' does only support monotonically increasing coordinates
                   (https://github.com/pydata/xarray/issues/4637)

    References:
     - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.interpolate_na.html
    """
    for dim in ('longitude', 'latitude'):
        if dim in dataset.dims:
            dataset = dataset.interpolate_na(dim=dim, use_coordinate=dim, fill_value="extrapolate", **kwargs)
    return dataset


def has_nan(dataarray_or_dataset):
    """Return True if the xarray.DataArray or any data variable of the xarray.Dataset contains NaN values"""
    if isinstance(dataarray_or_dataset, xarray.Dataset):
        return any(has_nan(dataarray_or_dataset[var]) for var in dataarray_or_dataset.data_vars)
    return bool(dataarray_or_dataset.isnull().any())


def _bracketing_slice(coord, value_min, value_max):
    """
    Return an index slice of the 1D coordinate which covers [value_min, value_max] including the next coordinate
    values beyond the bounds (if available). Works with ascending and descending coordinates.
    """
    values = coord.values
    n = len(values)
    descending = n > 1 and values[0] > values[-1]
    if descending:
        values = values[::-1]
    lower = max(np.searchsorted(values, value_min, side='right') - 1, 0)
    upper = min(np.searchsorted(values, value_max, side='left'), n - 1)
    if descending:
        lower, upper = n - 1 - upper, n - 1 - lower
    return slice(lower, upper + 1)


def _dimension_indexers(dataset, indexers):
    """Drop indexers which are not dimensions of the dataset"""
    ignored = [name for name in indexers if name not in dataset.dims]
    if ignored:
        logger.warning(f"Ignoring indexers which are not dimensions of the dataset: {ignored}")
    return {name: value for name, value in indexers.items() if name in dataset.dims}


def _value_range(value):
    """Return (min, max) of a scalar, sequence or slice indexer, independent of its order"""
    if value is None:
        return None
    if isinstance(value, slice):
        if value.start is None or value.stop is None:
            return None
        value = [value.start, value.stop]
    values = np.asarray(value, dtype=float)
    return values.min().item(), values.max().item()
