import logging

import copernicusmarine

from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)


class DownloaderCopernicusMarineToolboxApi(DownloaderBase):
    """
    Downloader class for CMEMs data using the Copernicus Marine Toolbox api
    
    https://pypi.org/project/copernicusmarine/
    """
    
    def __init__(self, cmems_username, cmems_password, **kwargs):
        super().__init__('cmtapi', username=cmems_username, password=cmems_password,**kwargs)
        self.client = copernicusmarine.login(username=cmems_username, password=cmems_password)
    
    def download(self, parameters=None, sel_dict=None, file_out=None):
        """
        :param parameters: list
        :param sel_dict: dict
            Dataset_id
            Coordinate selection by value, e.g. {'longitude': slice(10.25, 20.75)} or {'longitude': 10.25}
            Time
            variables
        :param file_out:
            File name used to save the dataset as NetCDF file. No file is saved without specifying file_out
        :return: xarray.Dataset    
            
        Reference:
         - https://help.marine.copernicus.eu/en/articles/7949409-copernicus-marine-toolbox-introduction
         - https://help.marine.copernicus.eu/en/articles/8612591-switching-from-current-to-new-services
         
        """
        try:
            dataset = copernicusmarine.open_dataset(
                dataset_id =  sel_dict["dataset_id"],
                minimum_longitude = sel_dict["longitude"].start,
                maximum_longitude = sel_dict["longitude"].stop,
                minimum_latitude = sel_dict["latitude"].start,
                maximum_latitude = sel_dict["latitude"].stop,
                start_datetime = sel_dict["time"].start,
                end_datetime = sel_dict["time"].stop,
                variables = parameters
            )
        except KeyError:
            raise ValueError("Make sure to pass all the necessary values to subset the data (dataset_id, and lat, long, time intervals)")
            
        # Save dataset into file
        if file_out:
            logger.info(f"Save dataset to '{file_out}'")
            dataset.to_netcdf(file_out)     
        return dataset
