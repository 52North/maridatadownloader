import logging

from maridatadownloader.base import Downloader
from maridatadownloader.registry import register
from maridatadownloader.utils import open_xarray_dataset, rename_if_present

logger = logging.getLogger(__name__)


@register('etopo')
class DownloaderETOPO(Downloader):
    """
    Downloader for topography and bathymetric data from NCEI (ETOPO 2022, 30 arc-seconds, bedrock elevation)

    References:
        - https://www.ncei.noaa.gov/products/etopo-global-relief-model
        - https://www.ngdc.noaa.gov/thredds/catalog/global/ETOPO2022/30s/30s_bed_elev_netcdf/catalog.html?dataset=globalDatasetScan/ETOPO2022/30s/30s_bed_elev_netcdf/ETOPO_2022_v1_30s_N90W180_bed.nc  # noqa
    """
    url = ('https://www.ngdc.noaa.gov/thredds/dodsC/global/ETOPO2022/30s/30s_bed_elev_netcdf'
           '/ETOPO_2022_v1_30s_N90W180_bed.nc')

    def __init__(self, chunks=None):
        """
        :param chunks: chunk sizes passed to xarray.open_dataset (requires dask)
        """
        self.chunks = chunks

    def open_dataset(self, request):
        if self._dataset is None:
            self._dataset = open_xarray_dataset(self.url, self.chunks)
        return self._dataset

    def normalize(self, dataset, request):
        return rename_if_present(dataset, {'lat': 'latitude', 'lon': 'longitude'})
