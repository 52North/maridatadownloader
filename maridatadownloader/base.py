import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, Self

import xarray

from maridatadownloader.subsetting import Subset

logger = logging.getLogger(__name__)

# Global attributes which are maintained by the netCDF-C library itself and must not be written back.
# References:
#  - https://github.com/pydata/xarray/issues/2822
#  - https://github.com/Unidata/netcdf4-python/issues/1020
RESERVED_NETCDF_ATTRS = ("_NCProperties", "_IsNetcdf4", "_SuperblockVersion")


@dataclass(frozen=True)
class Request:
    """What the user asked for. It is passed to all hooks so that a data source can react to it."""

    parameters: tuple[str, ...] = ()
    subset: Subset | None = None

    @classmethod
    def create(
        cls,
        parameters: str | Iterable[str] | None = None,
        subset: Subset | None = None,
    ) -> Self:
        if isinstance(parameters, str):
            parameters = (parameters,)
        return cls(tuple(parameters or ()), subset)

    def time_bounds(self) -> tuple[datetime, datetime] | None:
        """Requested time range as timezone-aware (UTC) datetime objects or None"""
        return self.subset.time_bounds() if self.subset else None

    def bbox(self) -> tuple[float, float, float, float] | None:
        """Requested bounding box as (lon_min, lat_min, lon_max, lat_max) or None"""
        return self.subset.bbox() if self.subset else None


class Downloader(ABC):
    """
    Downloader base class.

    Subclasses have to implement `open_dataset`, which returns the (lazy) source dataset as xarray.Dataset.
    Optionally, they can override the hooks `select_parameters`, `normalize` and `postprocess`.

    `get_xarray_dataset` applies the following pipeline:
        open_dataset -> select_parameters -> normalize -> subset.apply -> postprocess

    `normalize` is expected to return a dataset following the package conventions, so that subsets can rely on them:
     - coordinates are named 'time', 'latitude' and 'longitude' (independently of the name in the source)
     - latitude is ascending and defined from -90° to 90°
     - longitude is ascending and defined from -180° to 180°
    """

    name: ClassVar[str] = ""

    _dataset: xarray.Dataset | None = None

    @abstractmethod
    def open_dataset(self, request: Request) -> xarray.Dataset:
        """Return the (lazy) source dataset. The request can be used to choose the source, e.g. URLs."""

    def select_parameters(
        self, dataset: xarray.Dataset, request: Request
    ) -> xarray.Dataset:
        """Select the requested parameters (data variables)"""
        if request.parameters:
            return dataset[list(request.parameters)]
        return dataset

    def normalize(self, dataset: xarray.Dataset, request: Request) -> xarray.Dataset:
        """Apply the package conventions before subsetting, e.g. rename coordinates or transform longitudes"""
        return dataset

    def postprocess(self, dataset: xarray.Dataset, request: Request) -> xarray.Dataset:
        """Apply operations on the subset, e.g. unit conversions"""
        return dataset

    def get_xarray_dataset(
        self,
        parameters: str | Iterable[str] | None = None,
        subset: Subset | None = None,
    ) -> xarray.Dataset:
        """
        :param parameters: str or list of str. All parameters are returned if None.
        :param subset: Subset, e.g. BoxSubset or TrajectorySubset. The whole dataset is returned if None.
        :return: xarray.Dataset (lazy if supported by the data source)
        """
        request = Request.create(parameters, subset)
        dataset = self.open_dataset(request)
        dataset = self.select_parameters(dataset, request)
        dataset = self.normalize(dataset, request)
        if subset is not None:
            dataset = subset.apply(dataset)
        return self.postprocess(dataset, request)

    def save_to_file(
        self,
        path: str | PathLike,
        parameters: str | Iterable[str] | None = None,
        subset: Subset | None = None,
        **to_netcdf_kwargs: Any,
    ) -> Path:
        """
        Save the dataset returned by `get_xarray_dataset` as NetCDF file.

        :param to_netcdf_kwargs: passed to xarray.Dataset.to_netcdf
        :return: pathlib.Path of the file
        """
        dataset = _drop_reserved_netcdf_attrs(
            self.get_xarray_dataset(parameters, subset)
        )
        logger.info(f"Save dataset to '{path}'")
        dataset.to_netcdf(path, **to_netcdf_kwargs)
        return Path(path)

    def close(self) -> None:
        """Release resources of the source dataset"""
        if self._dataset is not None:
            self._dataset.close()
            self._dataset = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def _drop_reserved_netcdf_attrs(dataset: xarray.Dataset) -> xarray.Dataset:
    if not any(attr in dataset.attrs for attr in RESERVED_NETCDF_ATTRS):
        return dataset
    dataset = dataset.copy(deep=False)
    dataset.attrs = {
        key: value
        for key, value in dataset.attrs.items()
        if key not in RESERVED_NETCDF_ATTRS
    }
    return dataset
