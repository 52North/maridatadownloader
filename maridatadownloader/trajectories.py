"""Helpers for enriching trajectories (e.g. ship positions) with environmental data"""

from datetime import UTC, datetime

import pandas as pd

from maridatadownloader.registry import get_downloader
from maridatadownloader.subsetting import TrajectorySubset


def enrich_trajectory_with_env_data(
    csv_file,
    username,
    password,
    method_interp="nearest",
    method_extrap="linear",
    columns=None,
):
    """
    Enrich the trajectory with weather (GFS), wave, physics and currents (CMEMS) data

    :param csv_file: csv file with positions (see `read_hf_data_positions`)
    :param username: CMEMS username
    :param password: CMEMS password
    :param method_interp: interpolation method along the trajectory
    :param method_extrap: extrapolation method used to fill NaN values in CMEMS data (e.g. on land pixels)
    :param columns: leading columns of the returned dataframe
    :return: pandas.DataFrame
    """
    if columns is None:
        columns = [
            "trajectory",
            "time",
            "longitude",
            "latitude",
            "depth",
            "height_above_ground",
        ]

    kwargs_cmems = {
        "username": username,
        "password": password,
        "method_interp": method_interp,
        "method_extrap": method_extrap,
    }
    df_currents = enrich_trajectory_with_currents_data(csv_file, **kwargs_cmems)
    df_physics = enrich_trajectory_with_physics_data(csv_file, **kwargs_cmems)
    df_wave = enrich_trajectory_with_wave_data(csv_file, **kwargs_cmems)
    df_weather = enrich_trajectory_with_weather_data(
        csv_file, method_interp=method_interp
    )

    # Merge dataframes
    data_frames = [
        df_weather,
        df_wave,
        df_physics.reset_index(),
        df_currents.reset_index(),
    ]
    df_env_data = pd.concat(data_frames, axis=1)
    # Remove duplicate columns
    df_env_data = df_env_data.loc[:, ~df_env_data.columns.duplicated()]
    # Change column order
    df_env_data = df_env_data[
        columns + [col for col in df_env_data.columns.to_list() if col not in columns]
    ]
    return df_env_data


def enrich_trajectory_with_currents_data(
    csv_file,
    username,
    password,
    parameters=None,
    method_interp="nearest",
    method_extrap="linear",
):
    """
    :return: pandas.DataFrame
    """
    if parameters is None:
        parameters = ["utotal", "vtotal"]
    return _enrich_trajectory_with_cmems_data(
        csv_file,
        "cmems_mod_glo_phy_anfc_merged-uv_PT1H-i",
        username,
        password,
        parameters,
        method_interp,
        method_extrap,
    )


def enrich_trajectory_with_physics_data(
    csv_file,
    username,
    password,
    parameters=None,
    method_interp="nearest",
    method_extrap="linear",
):
    """
    :return: pandas.DataFrame
    """
    if parameters is None:
        parameters = ["thetao", "so", "zos"]
    return _enrich_trajectory_with_cmems_data(
        csv_file,
        "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
        username,
        password,
        parameters,
        method_interp,
        method_extrap,
    )


def enrich_trajectory_with_wave_data(
    csv_file,
    username,
    password,
    parameters=None,
    method_interp="nearest",
    method_extrap="linear",
):
    """
    :return: pandas.DataFrame
    """
    if parameters is None:
        parameters = ["VHM0", "VMDR", "VTPK"]
    return _enrich_trajectory_with_cmems_data(
        csv_file,
        "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
        username,
        password,
        parameters,
        method_interp,
        method_extrap,
    )


def enrich_trajectory_with_weather_data(
    csv_file, parameters=None, height_above_ground=10, method_interp="nearest"
):
    """
    :return: pandas.DataFrame
    """
    if parameters is None:
        parameters = [
            "Temperature_surface",
            "Pressure_reduced_to_MSL_msl",
            "Wind_speed_gust_surface",
            "u-component_of_wind_height_above_ground",
            "v-component_of_wind_height_above_ground",
        ]

    extra = {"height_above_ground": height_above_ground} if height_above_ground else {}
    subset = TrajectorySubset.from_dataframe(
        read_hf_data_positions(csv_file), method=method_interp, extra=extra
    )
    gfs = get_downloader("gfs")
    return gfs.get_xarray_dataset(parameters=parameters, subset=subset).to_dataframe()


def read_hf_data_positions(csv_file):
    """
    :param csv_file:
    :return: pandas.DataFrame
    """
    df_positions = pd.read_csv(
        csv_file,
        names=["time", "latitude", "direction_y", "longitude", "direction_x"],
        sep=r"\||,",
        dtype=str,
        engine="python",
    )
    df_positions["latitude"] = df_positions["latitude"].map(
        lambda item: int(item[:2]) + float(item[2:]) / 60
    )
    df_positions["longitude"] = df_positions["longitude"].map(
        lambda item: int(item[:3]) + float(item[3:]) / 60
    )
    df_positions["time"] = df_positions["time"].map(
        lambda item: datetime.strptime(item, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC)
    )
    is_west = df_positions["direction_x"] == "W"
    df_positions.loc[is_west, "longitude"] = df_positions["longitude"][is_west] * -1
    is_south = df_positions["direction_y"] == "S"
    df_positions.loc[is_south, "latitude"] = df_positions["latitude"][is_south] * -1
    df_positions.drop(columns=["direction_y", "direction_x"], inplace=True)
    return df_positions


def _enrich_trajectory_with_cmems_data(
    csv_file, dataset_id, username, password, parameters, method_interp, method_extrap
):
    # CMEMS data has NaN values on land pixels, thus we need to extrapolate NaN values close to the coast to make
    # sure that we have no NaN values in the interpolated data for the trajectory
    subset = TrajectorySubset.from_dataframe(
        read_hf_data_positions(csv_file), method=method_interp, fill_nan=method_extrap
    )
    cmems = get_downloader(
        "cmems", dataset_id=dataset_id, username=username, password=password
    )
    return cmems.get_xarray_dataset(parameters=parameters, subset=subset).to_dataframe()
