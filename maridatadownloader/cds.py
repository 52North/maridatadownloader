import logging
import tempfile
from pathlib import Path

import pandas as pd
import xarray

from maridatadownloader.base import Downloader
from maridatadownloader.registry import register
from maridatadownloader.utils import rename_if_present, standardize_lat_lon

logger = logging.getLogger(__name__)


@register("era5")
class DownloaderERA5(Downloader):
    """
    Downloader for ERA5 reanalysis data from the Copernicus Climate Data Store (CDS)

    In contrast to the other downloaders, the data is not lazy-loaded. Instead, a CDS request is built from the
    requested parameters, time range and bounding box, and the result is downloaded and loaded into memory.
    Because a CDS request is the cartesian product of years, months, days and hours, more data than requested
    might be downloaded. The subset is applied afterwards as for all other downloaders.

    Note that parameters have to be given as CDS variable names (e.g. '10m_u_component_of_wind') while the
    returned dataset uses the short names (e.g. 'u10').

    References:
     - https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
     - https://cds.climate.copernicus.eu/how-to-api
    """

    url = "https://cds.climate.copernicus.eu/api"

    def __init__(
        self,
        api_key,
        dataset_name="reanalysis-era5-single-levels",
        product_type="reanalysis",
    ):
        """
        :param api_key: CDS personal access token
        :param dataset_name: CDS dataset name
        :param product_type: CDS product type
        """
        self.api_key = api_key
        self.dataset_name = dataset_name
        self.product_type = product_type

    def open_dataset(self, request):
        # Imported here because the client is only needed for downloading
        import cdsapi

        client = cdsapi.Client(url=self.url, key=self.api_key)
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "era5.nc"
            client.retrieve(
                self.dataset_name, self.build_cds_request(request), str(target)
            )
            logger.info("Download successful")
            with xarray.open_dataset(target) as dataset:
                return dataset.load()

    def build_cds_request(self, request):
        """Build the CDS request from the parameters, time range and bounding box of the request"""
        if not request.parameters:
            raise ValueError(
                "Parameters (CDS variable names) must be provided for ERA5"
            )
        time_bounds = request.time_bounds()
        if time_bounds is None:
            raise ValueError("A time range must be provided for ERA5")
        time_start, time_end = time_bounds
        # ERA5 is hourly, so include the full hours covering the time range
        times = pd.date_range(
            pd.Timestamp(time_start).floor("h"),
            pd.Timestamp(time_end).ceil("h"),
            freq="h",
        )
        cds_request = {
            "product_type": [self.product_type],
            "variable": list(request.parameters),
            "year": sorted({f"{t.year}" for t in times}),
            "month": sorted({f"{t.month:02d}" for t in times}),
            "day": sorted({f"{t.day:02d}" for t in times}),
            "time": sorted({f"{t.hour:02d}:00" for t in times}),
            "data_format": "netcdf",
            "download_format": "unarchived",
        }
        bbox = request.bbox()
        if bbox is not None:
            lon_min, lat_min, lon_max, lat_max = bbox
            cds_request["area"] = [lat_max, lon_min, lat_min, lon_max]
        return cds_request

    def select_parameters(self, dataset, request):
        """The parameters are already selected by the CDS request (and use different names in the dataset)"""
        return dataset

    def normalize(self, dataset, request):
        """
        ERA5 data come in ranges from 90 to -90 for latitude and, if no area is requested, from 0 to 360 for
        longitude. Convert them to ranges (-90, 90) for latitude and (-180, 180) for longitude.
        """
        return standardize_lat_lon(rename_if_present(dataset, {"valid_time": "time"}))
