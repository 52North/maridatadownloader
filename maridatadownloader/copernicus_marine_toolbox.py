import logging

import copernicusmarine

from maridatadownloader.xarray import DownloaderXarray

logger = logging.getLogger(__name__)


class DownloaderCopernicusMarineToolboxApi(DownloaderXarray):
    """
    Downloader class for CMEMS data using the Copernicus Marine Toolbox API

    The download method provides a convenient way to download data using lazy-loading. For more details check the
    documentation of the base class `DownloaderXarray` and the references below.

    References:
        - https://pypi.org/project/copernicusmarine/
        - https://help.marine.copernicus.eu/en/articles/7949409-copernicus-marine-toolbox-introduction
        - https://help.marine.copernicus.eu/en/articles/8612591-switching-from-current-to-new-services
    """
    # ToDo: make DownloaderXarray or parts of it a Mixin?
    def __init__(self, username, password, **kwargs):
        self.downloader_type = 'cmtapi'
        self.platform = 'cmems'
        self.username = username
        self.password = password
        self.product = kwargs.get('product')
        self.product_type = kwargs.get('product_type')
        self.dataset = None
        if 'chunks' in kwargs:
            self.chunks = kwargs['chunks']
        else:
            self.chunks = None
        self.client = copernicusmarine.login(username=username, password=password, force_overwrite=True)
        if self.product:
            self.open_dataset()

    def get_filename_or_obj(self, **kwargs):
        # FIXME: this is inconsistent with the parent class at the moment
        return None

    def download(self, parameters=None, sel_dict=None, isel_dict=None, file_out=None, interpolate=False, **kwargs):
        """
        :param parameters: str or list
        :param sel_dict: dict
            Coordinate selection by value, e.g. {'longitude': slice(10.25, 20.75)} or {'longitude': 10.25}
        :param isel_dict: dict
            Coordinate selection by index e.g. {'longitude': slice(0, 10)} or {'longitude': 0}
        :param file_out:
            File name used to save the dataset as NetCDF file. No file is saved without specifying file_out
        :param interpolate: bool
            Set to True if data should be downloaded off-grid so that interpolation is applied. Note that the
            sel method supports inexact matches but doesn't support actual interpolation (off-grid values).
            Also note that xarray.Dataset.interp expects - for interpolation over a datetime-like coordinate -
            the coordinates to be either datetime strings or datetimes.
        :param kwargs:
            Additional keyword arguments are passed to the corresponding method sel, isel or interp.
            For details on which arguments can be used check the references below.
        :return: xarray.Dataset

        References:
         - https://docs.xarray.dev/en/stable/user-guide/indexing.html
         - https://docs.xarray.dev/en/latest/generated/xarray.Dataset.sel.html
         - https://docs.xarray.dev/en/latest/generated/xarray.Dataset.isel.html
         - https://docs.xarray.dev/en/latest/user-guide/interpolation.html
         - https://docs.xarray.dev/en/latest/generated/xarray.Dataset.interp.html
        """
        product = kwargs.get('product')
        if product:
            self.product = product
            self.open_dataset()
        elif self.product is None:
            msg = "No product has been set yet!"
            logger.error(msg)
            raise ValueError(msg)

        coord_dict, subsetting_method = self._prepare_download(sel_dict, isel_dict, interpolate)

        try:
            dataset = self.preprocessing(self.dataset, parameters=parameters, coord_dict=coord_dict)
        except NotImplementedError:
            dataset = self.dataset

        dataset_sub = self._apply_subsetting(dataset, parameters, coord_dict, subsetting_method, **kwargs)

        try:
            dataset_sub = self.postprocessing(dataset_sub)
        except NotImplementedError:
            pass

        if file_out:
            logger.info(f"Save dataset to '{file_out}'")
            dataset_sub.to_netcdf(file_out)

        return dataset_sub

    def open_dataset(self, filename_or_obj=None):
        try:
            self.dataset = copernicusmarine.open_dataset(dataset_id=self.product)
        except Exception as err:
            raise err

    def set_product(self, product):
        self.product = product
        self.open_dataset()

    def set_product_type(self, product_type):
        self.product_type = product_type
