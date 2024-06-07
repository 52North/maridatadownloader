import io
import logging

import cdsapi
import requests
import xarray as xr
from datetime import datetime, timedelta

from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)


class DownloaderCdsApiERA5(DownloaderBase):
    """
    ERA5 Reanalysis Downloader from cds copernicus based on xarray

    https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
    """
    def __init__(self, uuid, api_key, **kwargs):
        super().__init__('cdsapi', username=uuid, password=api_key, **kwargs)
        self.platform = 'era5'
        self.dataset = None
        self.url = 'https://cds.climate.copernicus.eu/api/v2'
        self.client = cdsapi.Client(url=self.url, key=f'{self.username}:{self.password}')

    def download(self, settings=None, file_out=None, parameters=None, sel_dict=None, **kwargs):
        """
        :param settings:
        :param file_out:
        :param kwargs:
        :return: xarray.Dataset
        """
        settings['product_type'] = 'reanalysis'
        settings['format'] = 'netcdf'
        
        # Apply subsetting on data
        try:
            settings = self._apply_subsetting(settings, parameters, sel_dict, **kwargs)      
        except Exception:
            'Make sure to pass filter parameters inside the sel_dict'
                        
        request = self.client.retrieve(name='reanalysis-era5-single-levels', request=settings)
        r = requests.get(request.location)
        if r.status_code == 200:
            logger.info('Download successful')
        nc_data = io.BytesIO(r.content)
        self.dataset = xr.open_dataset(nc_data)
        logger.info('Data read successful')
        
        try:
            dataset_sub = self.postprocessing(self.dataset)
        except NotImplementedError:
            pass

        if file_out:
            logger.info(f"Save dataset to '{file_out}'")
            dataset_sub.to_netcdf(file_out)
        return dataset_sub

    def postprocessing(self, dataset, **kwargs):
        """
        ERA5 data come in ranges from 90 to -90 for latitude. Convert dataset globally to ranges (-90, 90) for latitude.
        """
        dataset = dataset.reindex(latitude=list(reversed(dataset.latitude)))
        return dataset
    
    def preprocessing(self, dataset, parameters=None, coord_dict=None):
        """Apply operations on the xarray.Dataset before download, e.g. transform coordinates"""
        raise NotImplementedError(".preprocessing() can optionally be overridden.")

    def _apply_subsetting(self, settings, parameters=None, sel_dict=None, **kwargs):
        """
        Apply subsetting on data considering other sel_dict already used on maridatadownloader tool
        """
        
        # Define variables to be downloaded 
        if parameters is not None:
            settings['variable'] = parameters
        else:
            settings['variable'] = ['10m_u_component_of_wind', '10m_v_component_of_wind']
        
        start_datetime_str = sel_dict['time'].start #start date
        end_datetime_str = sel_dict['time'].stop # end date
        
        # Parse the datetime strings to datetime objects
        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
        
        # Generate list of hourly timestamps
        timestamps = [start_datetime + timedelta(hours=i) for i in range(int((end_datetime - start_datetime).total_seconds() // 3600) + 1)]

        settings['year'] = list(set([str(timestamps[i].year) for i in range(0,len(timestamps))]))
        settings['month'] = sorted(list(set([str(timestamps[i].month) if len(str(timestamps[i].month)) >= 2 else '0'+str(timestamps[i].month) for i in range(0,len(timestamps))])))
        settings['day'] = sorted(list(set([str(timestamps[i].day) if len(str(timestamps[i].day)) >= 2 else '0'+str(timestamps[i].day) for i in range(0,len(timestamps))])))
        # Subset based on time (00:00)
        settings['time'] = sorted(list(set([dt.strftime("%H:00") for dt in timestamps])))
        
        return settings
