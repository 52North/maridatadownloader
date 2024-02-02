import io
import logging

import xarray as xr
import copernicusmarine


from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)



class DownloaderCopernicusMarineToolboxapi(DownloaderBase):
    """
    Downloader class for CMEMs data using the Copernicus Marine Toolbox api
    
    https://pypi.org/project/copernicusmarine/
    """
    
    def __init__(self, cmems_username, cmems_password, **kwargs):
        super().__init__('cmtapi', username=cmems_username, password=cmems_password,**kwargs)
        self.client = copernicusmarine.login(username=cmems_username, password=cmems_password)
    
    def download(self, sel_dict=None, file_out=None):
        """
        :param sel_dict: dict
            Dataset_id
            Coordinate selection by value, {'longitude': [min_value, max_value]} e.g. {'longitude': [10.25, 20.75]}
            Time
            variables
        :param file_out:
            File name used to save the dataset as NetCDF file. No file is saved without specifying file_out
        :return: xarray.Dataset    
            
        Reference:
         - https://help.marine.copernicus.eu/en/articles/7949409-copernicus-marine-toolbox-introduction
         - https://help.marine.copernicus.eu/en/articles/8612591-switching-from-current-to-new-services
          
        """
        dataset = copernicusmarine.open_dataset(
            dataset_id =  sel_dict["dataset_id"],
            minimum_longitude = sel_dict["longitude"][0],
            maximum_longitude = sel_dict["longitude"][1],
            minimum_latitude = sel_dict["latitude"][0],
            maximum_latitude = sel_dict["latitude"][1],
            start_datetime = sel_dict["time"][0],
            end_datetime = sel_dict["time"][1],
            variables = sel_dict["variables"]
        )
        
        # Save dataset into file
        if file_out:
            logger.info(f"Save dataset to '{file_out}'")
            dataset.to_netcdf(file_out)     
        return dataset