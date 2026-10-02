import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from io import IOBase
from os import PathLike
from typing import Any

import numpy as np
import pandas as pd
import xarray
from xarray.backends import AbstractDataStore

logger = logging.getLogger(__name__)


def open_xarray_dataset(
    filename_or_obj: str
    | PathLike
    | IOBase
    | bytes
    | memoryview
    | AbstractDataStore
    | list[str | PathLike | IOBase],
    chunks: int | str | dict | None = None,
) -> xarray.Dataset:
    """
    Open a single source with xarray.open_dataset or a list of sources with xarray.open_mfdataset

    :param filename_or_obj: source (e.g. path or OPeNDAP URL) or list of sources
    :param chunks: chunk sizes passed to xarray. Requires dask if not None. xarray.open_mfdataset always uses dask.
    """
    if isinstance(filename_or_obj, list):
        return xarray.open_mfdataset(
            filename_or_obj, decode_coords="all", chunks=chunks
        )
    return xarray.open_dataset(filename_or_obj, decode_coords="all", chunks=chunks)


def rename_if_present(
    dataset: xarray.Dataset, name_dict: dict[str, str]
) -> xarray.Dataset:
    """Rename variables/coordinates/dimensions, ignoring names which are not part of the dataset"""
    name_dict = {
        old: new
        for old, new in name_dict.items()
        if old in dataset.variables or old in dataset.dims
    }
    return dataset.rename(name_dict) if name_dict else dataset


def standardize_lat_lon(dataset: xarray.Dataset) -> xarray.Dataset:
    """Convert longitudes to the range (-180, 180) and sort latitude and longitude in ascending order"""
    with xarray.set_options(keep_attrs=True):
        dataset = dataset.assign_coords(
            longitude=((dataset.longitude + 180) % 360) - 180
        )
    return dataset.sortby(["latitude", "longitude"])


def rename_numbered_variants(
    dataset: xarray.Dataset, base_names: Iterable[str]
) -> xarray.Dataset:
    """
    Rename numbered variants of coordinates to their base name, e.g. 'time1' -> 'time', as long as the base name is
    not already in use. If several variants exist, the one with the lowest number is renamed.
    """
    names = set(dataset.variables) | set(dataset.dims)
    for base_name in base_names:
        if base_name in names:
            continue
        variants = sorted(
            (
                name
                for name in names
                if re.fullmatch(rf"{re.escape(base_name)}\d+", name)
            ),
            key=lambda name: int(name[len(base_name) :]),
        )
        if variants:
            if len(variants) > 1:
                logger.warning(
                    f"Found several variants {variants} for '{base_name}', renaming only '{variants[0]}'"
                )
            dataset = dataset.rename({variants[0]: base_name})
            names = set(dataset.variables) | set(dataset.dims)
    return dataset


def to_naive_utc(value: Any) -> Any:
    """
    Convert timezone-aware datetime objects (also inside slices, lists and tuples) to timezone-naive UTC datetime
    objects because xarray.Dataset.sel cannot compare them with timezone-naive datetime64 coordinates.
    """
    if isinstance(value, slice):
        return slice(to_naive_utc(value.start), to_naive_utc(value.stop), value.step)
    if isinstance(value, (list, tuple)):
        return type(value)(to_naive_utc(item) for item in value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def to_utc_index(values: Any) -> pd.DatetimeIndex:
    """
    Convert time values to a timezone-aware (UTC) pandas.DatetimeIndex. Supported are e.g. strings, datetime objects
    (timezone-naive values are interpreted as UTC), numpy.datetime64, sequences of those and xarray.DataArray.
    """
    if isinstance(values, xarray.DataArray):
        values = values.values
    if np.ndim(values) == 0:
        values = [values]
    # Don't convert to a numpy array first, which might coerce mixed types (e.g. str and datetime) to str
    return pd.DatetimeIndex(pd.to_datetime(values, utc=True))


def time_range(value: Any) -> tuple[datetime, datetime] | None:
    """
    Return the start and end time of a time indexer (scalar, sequence or slice) as timezone-aware datetime objects.

    :returns: 2-tuple of datetime.datetime objects or None if the range is not bounded on both sides
    """
    if isinstance(value, slice):
        if value.start is None or value.stop is None:
            return None
        value = [value.start, value.stop]
    times = to_utc_index(value)
    return times.min().to_pydatetime(), times.max().to_pydatetime()
