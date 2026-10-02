import logging
from datetime import UTC, datetime, time, timedelta
from math import ceil

import xarray

from maridatadownloader.base import Downloader, Request
from maridatadownloader.registry import register
from maridatadownloader.utils import (
    open_xarray_dataset,
    rename_if_present,
    rename_numbered_variants,
    standardize_lat_lon,
)

logger = logging.getLogger(__name__)

# Some parameters use numbered coordinates, e.g. 'time1' instead of 'time'
NUMBERED_COORDS = ("time", "reftime", "height_above_ground")


@register("gfs")
class DownloaderGFS(Downloader):
    """
    Downloader for the Global Forecast System (GFS)

    By default, the GFS weather forecast data (-2 days up to +16 days) is used. If the requested time range
    ends more than `archive_age` in the past, archived forecast data is used instead.

    References:
     - https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php
     - https://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p25deg/catalog.html
     - https://rda.ucar.edu/datasets/d084001/
    """

    forecast_url = (
        "https://thredds.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best"
    )
    archive_age = timedelta(days=3)
    model_cycles = (time(0), time(6), time(12), time(18))
    forecast_times = (
        time(0),
        time(3),
        time(6),
        time(9),
        time(12),
        time(15),
        time(18),
        time(21),
    )

    def __init__(self, chunks: int | str | dict | None = None) -> None:
        """
        :param chunks: chunk sizes passed to xarray.open_dataset (requires dask). Archived data is always opened
            with dask chunks to be able to concatenate the files lazily.
        """
        self.chunks = chunks
        self._archive_datasets: list[xarray.Dataset] = []

    def open_dataset(self, request: Request) -> xarray.Dataset:
        time_bounds = request.time_bounds()
        if time_bounds and time_bounds[1] < datetime.now(UTC) - self.archive_age:
            logger.info("Access archived GFS data")
            return self._open_archive(*time_bounds, request.parameters)
        if self._dataset is None:
            self._dataset = open_xarray_dataset(self.forecast_url, self.chunks)
        return self._dataset

    def normalize(self, dataset: xarray.Dataset, request: Request) -> xarray.Dataset:
        """
        GFS data come in ranges from 90 to -90 for latitude and from 0 to 360 for longitude.
        Convert them to ranges (-90, 90) for latitude and (-180, 180) for longitude to make requests across the
        meridian easier to handle. Also rename numbered coordinates (e.g. 'time1' -> 'time').
        """
        dataset = rename_if_present(dataset, {"lat": "latitude", "lon": "longitude"})
        dataset = rename_numbered_variants(dataset, NUMBERED_COORDS)
        return standardize_lat_lon(dataset)

    def close(self) -> None:
        super().close()
        for dataset in self._archive_datasets:
            dataset.close()
        self._archive_datasets = []

    def _open_archive(
        self, time_start: datetime, time_end: datetime, parameters: tuple[str, ...]
    ) -> xarray.Dataset:
        urls = self._get_urls_time_window(time_start, time_end)
        if not urls:
            raise ValueError(
                f"No archived GFS data found for {time_start} - {time_end}"
            )
        datasets = []
        for url in urls:
            dataset = open_xarray_dataset(
                url, self.chunks if self.chunks is not None else {}
            )
            self._archive_datasets.append(dataset)
            if parameters:
                dataset = dataset[list(parameters)]
            # Each file contains one time step. Rename numbered coordinates first so all files share 'time'.
            datasets.append(rename_numbered_variants(dataset, NUMBERED_COORDS))
        return xarray.concat(
            datasets,
            dim="time",
            data_vars="minimal",
            coords="minimal",
            compat="override",
        )

    def _get_url(self, datetime_obj: datetime) -> str:
        """E.g. https://thredds.rda.ucar.edu/thredds/dodsC/files/g/d084001/2023/20230501/gfs.0p25.2023050100
        .f000.grib2"""
        if datetime_obj.time() in self.model_cycles:
            hour = f"{datetime_obj.hour:02d}"
            forecast_time = "f000"
        elif datetime_obj.time() in self.forecast_times:
            # Use the 3h forecast of the previous model cycle
            hour = f"{datetime_obj.hour - 3:02d}"
            forecast_time = "f003"
        else:
            raise ValueError(f"{datetime_obj} is not a GFS forecast time")
        date = datetime_obj.strftime("%Y%m%d")
        return (
            f"https://thredds.rda.ucar.edu/thredds/dodsC/files/g/d084001/{datetime_obj.year}/{date}/"
            f"gfs.0p25.{date}{hour}.{forecast_time}.grib2"
        )

    def _get_urls_time_window(
        self, time_start: datetime, time_end: datetime
    ) -> list[str]:
        """
        Return a list of urls for the specified time interval. If time_start and time_end coincide exactly with the
        forecast times of GFS, they will be used as interval bounds. If time_start or time_end do not coincide with
        forecast times, the url for the next smallest/largest forecast time is also included in the interval.
        """
        smaller_forecast_times = [
            element for element in self.forecast_times if element <= time_start.time()
        ]
        if not smaller_forecast_times:
            return []
        closest_smaller_gfs_datetime = datetime.combine(
            time_start.date(), max(smaller_forecast_times), tzinfo=UTC
        )
        urls = []
        for n in range(
            ceil((time_end - closest_smaller_gfs_datetime) / timedelta(hours=3)) + 1
        ):
            urls.append(
                self._get_url(closest_smaller_gfs_datetime + n * timedelta(hours=3))
            )
        return urls
