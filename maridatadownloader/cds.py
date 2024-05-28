import io
import logging

import cdsapi
import requests
import xarray as xr
from datetime import datetime, timedelta

from maridatadownloader.base import DownloaderBase

logger = logging.getLogger(__name__)


class   DownloaderCdsApiERA5(DownloaderBase):
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
        # Apply subsetting od data
        
        dataset = self.dataset
        
        dataset_sub = self._apply_subsetting_example(dataset,  parameters, sel_dict, **kwargs)
                                                    
        request = self.client.retrieve(name='reanalysis-era5-single-levels', request=settings)
        r = requests.get(request.location)
        if r.status_code == 200:
            logger.info('Download successful')
        nc_data = io.BytesIO(r.content)
        self.dataset = xr.open_dataset(nc_data)
        logger.info('Data read successful')
        
        try:
            dataset_sub = self.postprocessing(dataset_sub)
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

    def _apply_subsetting_example(self, settings, parameters=None, sel_dict=None, **kwargs):
        
        # Define variables to be downloaded 
        if parameters != None:
            settings['variable'] = parameters
        else:
            settings['variable'] = ['10m_u_component_of_wind', '10m_v_component_of_wind']
        
        # Define bbox to download the data
        # Format: [North, West, South, East]
        # lat_max, long_min, lat_min, long_max
        # y_max, x_min, y_min, x_max
        # area = [41, -75, 40, -73]
        # settings['area'] = [sel_dict['latitude'][1], 
        #                     sel_dict['longitude'][0],
        #                     sel_dict['latitude'][0],
        #                     sel_dict['longitude'][1]]
        
        # Subset timestamps
        # datetime list
        start_datetime_str = sel_dict['time'].start #start date
        end_datetime_str = sel_dict['time'].end # end date
        
        # Parse the datetime strings to datetime objects
        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%dT%H:%M:%S")
        end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%dT%H:%M:%S")
        
        # Generate list of hourly timestamps
        timestamps = [start_datetime + timedelta(hours=i) for i in range(int((end_datetime - start_datetime).total_seconds() // 3600) + 1)]

        settings['year'] = list(set([str(timestamps[i].year) for i in range(0,len(timestamps))]))
        settings['month'] = sorted(list(set([str(timestamps[i].month) if len(str(timestamps[i].month)) >= 2 else '0'+str(timestamps[i].month) for i in range(0,len(timestamps))])))
        settings['day'] = sorted(list(set([str(timestamps[i].day) if len(str(timestamps[i].day)) >= 2 else '0'+str(timestamps[i].day) for i in range(0,len(timestamps))])))
        # Subset based on time (00:00)
        settings['time'] = sorted(list(set([dt.strftime("%H:00") for dt in timestamps])))
        
        
        return settings
        
        
        
    # def _apply_subsetting(self, dataset, sel_dict,  parameters=None, coord_dict=None, subsetting_method=None, **kwargs):
        
    #     settings = {
    #         'product_type': 'reanalysis',    # Product type
    #         'variable': [
    #             '10m_u_component_of_wind',   # U-component of wind
    #             '10m_v_component_of_wind'    # V-component of wind
    #         ],
    #         'year': '2020',                  # Year of interest
    #         'month': '01',                   # Month of interest
    #         'day': '01',                     # Day of interest
    #         'time': '12:00',                 # Time of interest
    #         'format': 'netcdf'                 # Desired output format
    #         }
        
        
        
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
