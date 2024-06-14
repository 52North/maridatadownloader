import io
import logging

import pandas as pd
import cdsapi
import requests
import xarray as xr
from datetime import datetime, timedelta
from copy import deepcopy

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

    def download(self, settings=None, file_out=None, parameters=None, sel_dict=None, isel_dict=None, interpolate=False, **kwargs):
        """
        :param settings:
        :param file_out:
        :param kwargs:
        :return: xarray.Dataset
        """
        settings['product_type'] = 'reanalysis'
        settings['format'] = 'netcdf'
        
        coord_dict, subsetting_method = self._prepare_download(sel_dict, isel_dict, interpolate)

        try:
            settings = self._apply_subsetting(settings, parameters, coord_dict, subsetting_method, **kwargs)      
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
        ERA5 data come in ranges from 90 to -90 for latitude and from 0 to 360 for longitude.
        Convert dataset globally to ranges (-90, 90) for latitude and (-180, 180) for longitude first
        to make requests across meridian easier to handle and be in conformance with BTO data.
        """
        dataset = dataset.reindex(latitude=list(reversed(dataset.latitude)))
        dataset = dataset.assign_coords(longitude=(((dataset.longitude + 180) % 360) - 180))
        return dataset
    
    def preprocessing(self, dataset, parameters=None, coord_dict=None):
        """Apply operations on the xarray.Dataset before download, e.g. transform coordinates"""
        raise NotImplementedError(".preprocessing() can optionally be overridden.")

    def _apply_subsetting(self, settings, parameters=None, coord_dict=None, subsetting_method=None, **kwargs):
        """
        Apply subsetting on data considering other sel_dict already used on maridatadownloader tool
        """
        
        # Define variables to be downloaded
        if parameters is not None:
            settings['variable'] = parameters
        else:
            settings['variable'] = ['10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature']

        # Create list with all timestamps present in sel_dict
        timestamps = pd.to_datetime(coord_dict['time']).to_list()

        # Include timestamps in correct format for the API request
        settings['year'] = list(set([str(timestamps[i].year) for i in range(0,len(timestamps))]))
        settings['month'] = sorted(list(set([str(timestamps[i].month) if len(str(timestamps[i].month)) >= 2 else '0'+str(timestamps[i].month) for i in range(0,len(timestamps))])))
        settings['day'] = sorted(list(set([str(timestamps[i].day) if len(str(timestamps[i].day)) >= 2 else '0'+str(timestamps[i].day) for i in range(0,len(timestamps))])))
        # Subset based on time (00:00)
        settings['time'] = sorted(list(set([dt.strftime("%H:00") for dt in timestamps])))
        
        return settings


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