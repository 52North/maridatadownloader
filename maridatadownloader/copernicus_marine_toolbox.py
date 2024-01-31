import io
import logging

import xarray as xr
import copernicusmarine


from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)



class DownloaderCopernicusMarineToolboxapi(DownloaderBase):
    """
    
    
    https://pypi.org/project/copernicusmarine/
    """
    
    def __init__(self, cmems_username, cmems_password, **kwargs):
        super().__init__('cmtapi', username=cmems_username, password=cmems_password,**kwargs)
        self.client = copernicusmarine.login(username=cmems_username, password=cmems_password)
    
    def download(self, sel_dict=None, file_out=None, **kwargs):
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
        
        if file_out:
            logger.info(f"Save dataset to '{file_out}'")
            dataset.to_netcdf(file_out)     
        return dataset


