import logging
from copy import deepcopy

import copernicusmarine

from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)

# ToDo: harmonize with OPeNDAP (remove code duplication) -> xarray downloader base class?


class DownloaderCopernicusMarineToolboxApi(DownloaderBase):
    """
    Downloader class for CMEMS data using the Copernicus Marine Toolbox API

    The download method provides a convenient way to download data using lazy-loading including some checks to add
    robustness and the possibility to apply preprocessing and postprocessing.
    If it doesn't suit the requirements of the use case, it is also possible to directly work on the xarray.Dataset
    (self.dataset) and use all the methods provided by xarray directly
    (https://docs.xarray.dev/en/latest/generated/xarray.Dataset.html#xarray.Dataset).

    Coordinate subsetting with xarray can be done in different ways, e.g.:
     - by value or by index
     - allowing only exact matches or inexact matches (including different methods for inexact matches)
     - allowing off-grid subsetting using interpolation (including different interpolation methods)
    The download method can be used in all the aforementioned ways. For details check the method documentation.

    References:
        - https://pypi.org/project/copernicusmarine/
        - https://help.marine.copernicus.eu/en/articles/7949409-copernicus-marine-toolbox-introduction
        - https://help.marine.copernicus.eu/en/articles/8612591-switching-from-current-to-new-services
    """
    
    def __init__(self, username, password, **kwargs):
        super().__init__('cmtapi', username=username, password=password, **kwargs)
        self.client = copernicusmarine.login(username=username, password=password, overwrite_configuration_file=True)
        self.dataset = None
        self.product = kwargs.get('product')
        self.product_type = kwargs.get('product_type')
        if self.product:
            self.open_dataset()
    
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

    def open_dataset(self):
        try:
            self.dataset = copernicusmarine.open_dataset(dataset_id=self.product)
        except Exception as err:
            raise err

    def postprocessing(self, dataset):
        """Apply operations on the xarray.Dataset after download, e.g. rename variables"""
        raise NotImplementedError(".postprocessing() can optionally be overridden.")

    def preprocessing(self, dataset, parameters=None, coord_dict=None):
        """Apply operations on the xarray.Dataset before download, e.g. transform coordinates"""
        raise NotImplementedError(".preprocessing() can optionally be overridden.")

    def set_product(self, product):
        self.product = product

    def set_product_type(self, product_type):
        self.product_type = product_type

    def _apply_subsetting(self, dataset, parameters=None, coord_dict=None, subsetting_method=None, **kwargs):
        # ToDo: support xarrray.Dataset.interp_like?
        # Apply parameter subsetting
        if parameters:
            dataset_sub = dataset[parameters]
        else:
            dataset_sub = dataset

        # Check if the selection keys are valid dimension names
        if coord_dict:
            for key in list(coord_dict.keys()):
                if key not in dataset_sub.dims:
                    del coord_dict[key]

        # Apply coordinate subsetting
        if subsetting_method == 'sel':
            dataset_sub = dataset_sub.sel(**coord_dict, **kwargs)
        elif subsetting_method == 'isel':
            dataset_sub = dataset_sub.isel(**coord_dict, **kwargs)
        elif subsetting_method == 'interp':
            dataset_sub = dataset_sub.interp(**coord_dict, **kwargs)

        return dataset_sub

    def _prepare_download(self, sel_dict=None, isel_dict=None, interpolate=False):
        # Make a copy of the sel/isel dict because key-value pairs might be deleted from it
        coord_dict = {}
        subsetting_method = None
        if sel_dict:
            assert not isel_dict, "sel_dict and isel_dict are mutually exclusive"
            coord_dict = deepcopy(sel_dict)
            subsetting_method = 'sel'
        if isel_dict:
            assert not sel_dict, "sel_dict and isel_dict are mutually exclusive"
            coord_dict = deepcopy(isel_dict)
            subsetting_method = 'isel'
        if interpolate:
            assert not isel_dict, "interpolation cannot be applied with index subsetting"
            subsetting_method = 'interp'
        return coord_dict, subsetting_method
