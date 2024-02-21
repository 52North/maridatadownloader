from datetime import datetime

import numpy as np
import pandas as pd
import xarray

from maridatadownloader import DownloaderFactory
import maridatadownloader.copernicus_marine_toolbox


def enrich_trajectory_with_env_data(csv_file, username, password, method_interp='nearest', method_extrap='linear',
                                    columns=None):
    """
    :return:
    """
    kwargs_gfs = {
        'csv_file': csv_file,
        'method_interp': method_interp,
    }

    kwargs_cmems = {
        'csv_file': csv_file,
        'username': username,
        'password': password,
        'method_interp': method_interp,
        'method_extrap': method_extrap
    }

    if columns is None:
        columns = ['trajectory', 'time', 'longitude', 'latitude', 'depth', 'height_above_ground']

    df_currents = enrich_trajectory_with_currents_data(**kwargs_cmems)
    df_physics = enrich_trajectory_with_physics_data(**kwargs_cmems)
    df_wave = enrich_trajectory_with_wave_data(**kwargs_cmems)
    df_weather = enrich_trajectory_with_weather_data(**kwargs_gfs)

    # Merge dataframes
    data_frames = [df_weather, df_wave, df_physics.reset_index(), df_currents.reset_index()]
    df_env_data = pd.concat(data_frames, axis=1)
    # Remove duplicate columns
    df_env_data = df_env_data.loc[:, ~df_env_data.columns.duplicated()]
    # Change column order
    df_env_data = df_env_data[columns + [col for col in df_env_data.columns.to_list() if col not in columns]]
    return df_env_data


def enrich_trajectory_with_currents_data(csv_file, username, password, parameters=None,
                                         method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['utotal', 'vtotal']

    df_positions = read_hf_data_positions(csv_file)
    sel_dict = get_trajectory_dict(df_positions)

    currents_trajectory = get_cmems_trajectory('cmems_mod_glo_phy_anfc_merged-uv_PT1H-i', 'nrt',
                                               username, password, parameters, sel_dict,
                                               method_interp=method_interp, method_extrap=method_extrap)

    df_currents = currents_trajectory.to_dataframe()
    return df_currents


def enrich_trajectory_with_physics_data(csv_file, username, password, parameters=None,
                                        method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['thetao', 'so', 'zos']

    df_positions = read_hf_data_positions(csv_file)
    sel_dict = get_trajectory_dict(df_positions)

    physics_trajectory = get_cmems_trajectory('cmems_mod_glo_phy_anfc_0.083deg_PT1H-m', 'nrt',
                                              username, password, parameters, sel_dict,
                                              method_interp=method_interp, method_extrap=method_extrap)

    df_physics = physics_trajectory.to_dataframe()
    return df_physics


def enrich_trajectory_with_wave_data(csv_file, username, password, parameters=None,
                                     method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['VHM0', 'VMDR', 'VTPK']

    df_positions = read_hf_data_positions(csv_file)
    sel_dict = get_trajectory_dict(df_positions)

    wave_trajectory = get_cmems_trajectory('cmems_mod_glo_wav_anfc_0.083deg_PT3H-i', 'nrt',
                                           username, password, parameters, sel_dict,
                                           method_interp=method_interp, method_extrap=method_extrap)

    df_wave = wave_trajectory.to_dataframe()
    # path, ext = os.path.splitext(csv_file)
    # csv_file_out = path + '_cmems_wave' + ext
    # df_wave.to_csv(csv_file_out)
    return df_wave


def enrich_trajectory_with_weather_data(csv_file, parameters=None, height_above_ground=10, method_interp='nearest'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ["Temperature_surface", "Pressure_reduced_to_MSL_msl", "Wind_speed_gust_surface",
                      "u-component_of_wind_height_above_ground", "v-component_of_wind_height_above_ground"]

    df_positions = read_hf_data_positions(csv_file)
    sel_dict = get_trajectory_dict(df_positions)

    sel_dict['time1'] = sel_dict['time']
    if height_above_ground:
        sel_dict['height_above_ground'] = height_above_ground
        sel_dict['height_above_ground2'] = height_above_ground

    gfs = DownloaderFactory.get_downloader('opendap', 'gfs')
    weather_trajectory = gfs.download(parameters=parameters, sel_dict=sel_dict, interpolate=True,
                                      method=method_interp)
    df_weather = weather_trajectory.to_dataframe()
    return df_weather


def fill_nan(dataset, **kwargs):
    """
    :param dataset: xarray.Dataset or xarray.DataArray
    :param kwargs: Extra keyword arguments passed to interpolate_na
                   Note that 'limit' does only support monotonically increasing coordinates
                   (https://github.com/pydata/xarray/issues/4637)

    References:
     - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.interpolate_na.html
    """
    # Use extrapolation (default linear) to fill NaN values
    # Note: changing the order of the methods changes the result
    dataset_extrapolated = dataset.interpolate_na(dim="longitude", use_coordinate="longitude",
                                                  fill_value="extrapolate", **kwargs)
    dataset_extrapolated = dataset_extrapolated.interpolate_na(dim="latitude", use_coordinate="latitude",
                                                               fill_value="extrapolate", **kwargs)

    # Alternative approach using backward and forward propagation of values to fill NaN values
    # Very similar to interpolate_na with method='nearest'.
    # References:
    #  - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.ffill.html
    #  - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.bfill.html
    # dataset_extrapolated = dataset.ffill(dim='longitude', **kwargs).bfill(dim='longitude', **kwargs)
    # dataset_extrapolated = dataset_extrapolated.ffill(dim='latitude', **kwargs).bfill(dim='latitude', **kwargs)

    return dataset_extrapolated


def get_cmems_sub_cube(downloader, parameters, time_min, time_max, lon_min, lon_max, lat_min, lat_max,
                       spatial_buffer=1):
    """
    :param downloader:
    :param parameters:
    :param lat_max:
    :param lat_min:
    :param lon_max:
    :param lon_min:
    :param time_max:
    :param time_min:
    :param spatial_buffer: in degrees
    :return:
    """
    time_next_lower = downloader.dataset.time.sel(time=time_min, method='ffill')
    time_next_upper = downloader.dataset.time.sel(time=time_max, method='bfill')

    sub_cube = downloader.download(parameters=parameters, sel_dict={'time': slice(time_next_lower, time_next_upper)})
    sub_cube = sub_cube.sel(latitude=slice(lat_min - spatial_buffer, lat_max + spatial_buffer),
                            longitude=slice(lon_min - spatial_buffer, lon_max + spatial_buffer))
    return sub_cube


def get_cmems_trajectory(product, product_type, username, password, parameters, sel_dict,
                         spatial_buffer=1, method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    cmems = DownloaderFactory.get_downloader('opendap', 'cmems', username, password,
                                             product=product, product_type=product_type)

    assert 'time' in sel_dict
    assert 'longitude' in sel_dict
    assert 'latitude' in sel_dict

    time_min = min(sel_dict['time'])
    time_max = max(sel_dict['time'])
    lon_min = min(sel_dict['longitude'])
    lon_max = max(sel_dict['longitude'])
    lat_min = min(sel_dict['latitude'])
    lat_max = max(sel_dict['latitude'])

    # CMEMS data has NaN values on land pixels, thus we need to extrapolate NaN values close to the coast to make
    # sure that we have no NaN values in the interpolated data for the trajectory
    sub_cube = get_cmems_sub_cube(cmems, parameters, time_min, time_max, lon_min, lon_max, lat_min, lat_max,
                                  spatial_buffer)
    if has_nan(sub_cube):
        sub_cube = fill_nan(sub_cube, method=method_extrap)
    dataset_trajectory = sub_cube.interp(**sel_dict, method=method_interp)
    return dataset_trajectory


def get_trajectory_dict(df_positions, every_nth_row=1):
    """
    Define parameters for enriching trajectory with environmental data
    :param df_positions:
    :param every_nth_row:
    :return:
    """
    lons = df_positions.iloc[::every_nth_row]['longitude'].tolist()
    lats = df_positions.iloc[::every_nth_row]['latitude'].tolist()
    times = [ts.to_pydatetime() for ts in df_positions.iloc[::every_nth_row]['time'].tolist()]
    lons_xr = xarray.DataArray(lons, dims=['trajectory'])
    lats_xr = xarray.DataArray(lats, dims=['trajectory'])
    times_xr = xarray.DataArray(times, dims=['trajectory'])
    sel_dict = {
        'time': times_xr,
        'longitude': lons_xr,
        'latitude': lats_xr
    }
    return sel_dict


def has_nan(dataarray_or_dataset):
    """
    :param dataarray_or_dataset:
    :return:
    """
    if isinstance(dataarray_or_dataset, xarray.Dataset):
        for var in dataarray_or_dataset.data_vars:
            number_nans = (np.isnan(dataarray_or_dataset[var])).sum().values.item()
            if number_nans > 0:
                return True
        return False
    else:
        number_nans = (np.isnan(dataarray_or_dataset)).sum().values.item()
        if number_nans > 0:
            return True
        else:
            return False


def read_hf_data_positions(csv_file):
    """
    :param csv_file:
    :return: pandas.Dataframe
    """
    df_positions = pd.read_csv(csv_file, names=['time', 'latitude', 'N', 'longitude', 'E'], sep='\||,', dtype=str,
                               engine='python')
    df_positions['latitude'] = df_positions['latitude'].map(lambda item: int(item[:2]) + float(item[2:]) / 60)
    df_positions['longitude'] = df_positions['longitude'].map(lambda item: int(item[:3]) + float(item[3:]) / 60)
    df_positions['time'] = df_positions['time'].map(lambda item: datetime.strptime(item, '%Y-%m-%dT%H:%M:%S.%f'))
    df_positions.drop(columns=['N', 'E'], inplace=True)
    return df_positions


def create_cmems_column(df_ship, dataset_cmems_VHM0, date):
    """
    
    """
    
    # create new df with obtained cmems values based on ship coords 
    df_new = pd.DataFrame(dataset_cmems_VHM0.sel(latitude=df_ship['latitude'].tolist(), longitude=df_ship['longitude'].tolist(), time=date, method='nearest').VHM0.values.diagonal(), columns = ['CMEMs_wave_height'])
    
    # concatenate original df_ship positions with new df of cmems info
    df_enriched = pd.concat([df_ship, df_new], axis=1)
    
    return df_enriched
    
    
    
def enrich_trajectory_with_wave_data_igor(csv_file, username, password, parameters=None,
                                     method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['VHM0', 'VMDR', 'VTPK']


    ################ Changed 
    df_positions = pd.read_csv(csv_file)
    df_positions.rename(columns={'timestampUtc' : 'time'}, inplace=True)
    df_positions['time'] = pd.to_datetime(df_positions['time'])
    ###############
    
    sel_dict = get_trajectory_dict(df_positions)

    wave_trajectory = get_cmems_trajectory('cmems_mod_glo_wav_anfc_0.083deg_PT3H-i', 'nrt',
                                           username, password, parameters, sel_dict,
                                           method_interp=method_interp, method_extrap=method_extrap)

    df_wave = wave_trajectory.to_dataframe()
    # path, ext = os.path.splitext(csv_file)
    # csv_file_out = path + '_cmems_wave' + ext
    # df_wave.to_csv(csv_file_out)
    return df_wave


def enrich_trajectory_with_weather_data_igor(csv_file, parameters=None, height_above_ground=10, method_interp='nearest'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ["Temperature_surface", "Pressure_reduced_to_MSL_msl", "Wind_speed_gust_surface",
                      "u-component_of_wind_height_above_ground", "v-component_of_wind_height_above_ground"]

    ################ Changed 
    df_positions = pd.read_csv(csv_file)
    df_positions.rename(columns={'timestampUtc' : 'time'}, inplace=True)
    df_positions['time'] = pd.to_datetime(df_positions['time'])
    ###############
    
    sel_dict = get_trajectory_dict(df_positions)

    sel_dict['time1'] = sel_dict['time']
    if height_above_ground:
        sel_dict['height_above_ground'] = height_above_ground
        sel_dict['height_above_ground2'] = height_above_ground

    gfs = DownloaderFactory.get_downloader('opendap', 'gfs')
    weather_trajectory = gfs.download(parameters=parameters, sel_dict=sel_dict, interpolate=True,
                                      method=method_interp)
    df_weather = weather_trajectory.to_dataframe()
    return df_weather


def enrich_trajectory_with_physics_data_igor(csv_file, username, password, parameters=None,
                                        method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['thetao', 'so', 'zos']

    ################ Changed 
    df_positions = pd.read_csv(csv_file)
    df_positions.rename(columns={'timestampUtc' : 'time'}, inplace=True)
    df_positions['time'] = pd.to_datetime(df_positions['time'])
    ###############
    
    sel_dict = get_trajectory_dict(df_positions)

    physics_trajectory = get_cmems_trajectory('cmems_mod_glo_phy_anfc_0.083deg_PT1H-m', 'nrt',
                                              username, password, parameters, sel_dict,
                                              method_interp=method_interp, method_extrap=method_extrap)

    df_physics = physics_trajectory.to_dataframe()
    return df_physics


def enrich_trajectory_with_physics_data_igor(csv_file, username, password, parameters=None,
                                        method_interp='nearest', method_extrap='linear'):
    """
    :return: pandas.Dataframe
    """
    if parameters is None:
        parameters = ['thetao', 'so', 'zos']

    ################ Changed 
    df_positions = pd.read_csv(csv_file)
    df_positions.rename(columns={'timestampUtc' : 'time'}, inplace=True)
    df_positions['time'] = pd.to_datetime(df_positions['time'])
    ###############
    
    sel_dict = get_trajectory_dict(df_positions)

    physics_trajectory = get_cmems_trajectory('cmems_mod_glo_phy_anfc_0.083deg_PT1H-m', 'nrt',
                                              username, password, parameters, sel_dict,
                                              method_interp=method_interp, method_extrap=method_extrap)

    df_physics = physics_trajectory.to_dataframe()
    return df_physics


def enrich_trajectory_with_bathymetric_data(csv_file, method_interp='nearest', method_extrap='linear', spatial_buffer=1):
    """
    :return: pandas.Dataframe
    """
    ################ Changed 
    df_positions = pd.read_csv(csv_file)
    df_positions.rename(columns={'timestampUtc' : 'time'}, inplace=True)
    df_positions['time'] = pd.to_datetime(df_positions['time'])
    ###############
    
    sel_dict = get_trajectory_dict(df_positions)


    data_url = "https://www.ngdc.noaa.gov/thredds/dodsC/global/ETOPO2022/30s/30s_geoid_netcdf/ETOPO_2022_v1_30s_N90W180_geoid.nc"
    bathymetric_data = xarray.open_dataset(data_url)
    
    
    # filter specific values based on interest lat / lon
    lon_min = min(sel_dict['longitude'])
    lon_max = max(sel_dict['longitude'])
    lat_min = min(sel_dict['latitude'])
    lat_max = max(sel_dict['latitude'])
    
    depth_trajectory = bathymetric_data.sel(lat=slice(lat_min - spatial_buffer, lat_max + spatial_buffer),
                            lon=slice(lon_min - spatial_buffer, lon_max + spatial_buffer))

    depth_trajectory = depth_trajectory.interp(method=method_interp)

    df_depth = depth_trajectory.to_dataframe()
    return df_depth
