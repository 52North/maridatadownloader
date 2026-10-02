import logging
from typing import Any

import xarray

from maridatadownloader.base import Downloader, Request
from maridatadownloader.registry import register

logger = logging.getLogger(__name__)


@register("cmems")
class DownloaderCMEMS(Downloader):
    """
    Downloader for CMEMS data using the Copernicus Marine Toolbox API (lazy-loading via
    copernicusmarine.open_dataset)

    The credentials are passed directly to copernicusmarine.open_dataset, so no credentials file is written
    (in contrast to copernicusmarine.login).

    References:
        - https://pypi.org/project/copernicusmarine/
        - https://help.marine.copernicus.eu/en/articles/7949409-copernicus-marine-toolbox-introduction
        - https://help.marine.copernicus.eu/en/articles/8612591-switching-from-current-to-new-services
    """

    def __init__(
        self,
        dataset_id: str,
        username: str | None = None,
        password: str | None = None,
        **open_kwargs: Any,
    ) -> None:
        """
        :param dataset_id: CMEMS dataset id, e.g. 'cmems_mod_glo_wav_anfc_0.083deg_PT3H-i'
        :param username: Copernicus Marine username. If None, the toolbox looks for stored credentials or
            environment variables.
        :param password: Copernicus Marine password
        :param open_kwargs: Additional keyword arguments passed to copernicusmarine.open_dataset
            (e.g. dataset_version)
        """
        self.dataset_id = dataset_id
        self.username = username
        self.password = password
        self.open_kwargs = open_kwargs

    def open_dataset(self, request: Request) -> xarray.Dataset:
        if self._dataset is None:
            # Imported here because importing the toolbox is slow
            import copernicusmarine

            self._dataset = copernicusmarine.open_dataset(
                dataset_id=self.dataset_id,
                username=self.username,
                password=self.password,
                **self.open_kwargs,
            )
        return self._dataset
