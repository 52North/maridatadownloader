from copy import deepcopy
from datetime import datetime, timedelta, timezone

import xarray
from numpy import datetime64, ndarray
from pandas import Timestamp


def convert_datetime(dt64):
    """Convert numpy.datetime64 to datetime.datetime

    :returns: datetime.datetime object or None
    """
    if dt64.dtype == '<M8[s]' or dt64.dtype == '<m8[s]':
        return datetime.fromtimestamp(dt64.astype(int), tz=timezone.utc)
    elif dt64.dtype == '<M8[ms]' or dt64.dtype == '<m8[ms]':
        return datetime.fromtimestamp(dt64.astype(int) * 1e-3, tz=timezone.utc)
    elif dt64.dtype == '<M8[us]' or dt64.dtype == '<m8[us]':
        return datetime.fromtimestamp(dt64.astype(int) * 1e-6, tz=timezone.utc)
    elif dt64.dtype == '<M8[ns]' or dt64.dtype == '<m8[ns]':
        return datetime.fromtimestamp(dt64.astype(int) * 1e-9, tz=timezone.utc)
    elif dt64.dtype == '<M8[ps]' or dt64.dtype == '<m8[ps]':
        return datetime.fromtimestamp(dt64.astype(int) * 1e-12, tz=timezone.utc)
    else:
        print("... do not know how to convert numpy.datetime64 with dtype '{}'"
              " to datetime.datetime object".format(dt64.dtype))
        return None


def get_sel_dict_orthogonal(sel_dict, buffer_space=1.0, buffer_hours=3):
    """
    :param sel_dict:
    :param buffer_space: numerical in degrees
    :param buffer_hours: numerical in hours
    :return: sel_dict for orthogonal indexing including a spatial and temporal buffer
    """
    sel_dict_orthogonal = deepcopy(sel_dict)
    if 'longitude' in sel_dict_orthogonal and isinstance(sel_dict_orthogonal['longitude'], xarray.DataArray):
        lon_min = min(sel_dict_orthogonal['longitude']).values.item() - buffer_space
        lon_max = max(sel_dict_orthogonal['longitude']).values.item() + buffer_space
        sel_dict_orthogonal['longitude'] = slice(lon_min, lon_max)
    if 'latitude' in sel_dict_orthogonal and isinstance(sel_dict_orthogonal['latitude'], xarray.DataArray):
        lat_min = min(sel_dict_orthogonal['latitude']).values.item() - buffer_space
        lat_max = max(sel_dict_orthogonal['latitude']).values.item() + buffer_space
        sel_dict_orthogonal['latitude'] = slice(lat_min, lat_max)
    # Make datetime objects timezone-unaware because the xarray.Dataset.sel method otherwise throws an error
    if 'time' in sel_dict_orthogonal and isinstance(sel_dict_orthogonal['time'], xarray.DataArray):
        time_start, time_end = get_start_and_end_time(sel_dict_orthogonal['time'])
        time_start = (time_start - timedelta(hours=buffer_hours)).replace(tzinfo=None)
        time_end = (time_end + timedelta(hours=buffer_hours)).replace(tzinfo=None)
        sel_dict_orthogonal['time'] = slice(time_start, time_end)
    if 'time1' in sel_dict_orthogonal and isinstance(sel_dict_orthogonal['time1'], xarray.DataArray):
        time_start, time_end = get_start_and_end_time(sel_dict_orthogonal['time1'])
        time_start = (time_start - timedelta(hours=buffer_hours)).replace(tzinfo=None)
        time_end = (time_end + timedelta(hours=buffer_hours)).replace(tzinfo=None)
        sel_dict_orthogonal['time1'] = slice(time_start, time_end)
    if 'time2' in sel_dict_orthogonal and isinstance(sel_dict_orthogonal['time2'], xarray.DataArray):
        time_start, time_end = get_start_and_end_time(sel_dict_orthogonal['time2'])
        time_start = (time_start - timedelta(hours=buffer_hours)).replace(tzinfo=None)
        time_end = (time_end + timedelta(hours=buffer_hours)).replace(tzinfo=None)
        sel_dict_orthogonal['time2'] = slice(time_start, time_end)
    return sel_dict_orthogonal


def get_start_and_end_time(time_):
    """Extract start and end time from

    :returns: 2-tuple of datetime.datetime objects
    """
    # Note: xarray doesn't support tuples as indexer
    if isinstance(time_, str):
        time_start = time_end = parse_datetime(time_)
    elif isinstance(time_, datetime):
        time_start = time_end = time_
    elif isinstance(time_, slice):
        time_start = time_.start
        time_end = time_.stop
        if isinstance(time_start, str):
            time_start = parse_datetime(time_start)
        if isinstance(time_end, str):
            time_end = parse_datetime(time_end)
    elif isinstance(time_, list) or isinstance(time_, ndarray):
        time_start = min(time_)
        time_end = max(time_)
        if isinstance(time_start, str):
            time_start = parse_datetime(time_start)
        if isinstance(time_end, str):
            time_end = parse_datetime(time_end)
    elif isinstance(time_, xarray.DataArray):
        # Notes:
        # - if datetime objects are timezone aware xarray.DataArray will parse them into pandas.Timestamp objects
        # - if datetime objects are not timezone aware xarray.DataArray will parse them into numpy.datetime64 objects
        time_start = min(time_).values
        time_end = max(time_).values
        if isinstance(time_start, ndarray):
            time_start = time_start.item()
            if isinstance(time_start, Timestamp):
                time_start = time_start.to_pydatetime()
            elif isinstance(time_start, str):
                time_start = parse_datetime(time_start)
        elif isinstance(time_start, datetime64):
            time_start = convert_datetime(time_start)
        if isinstance(time_end, ndarray):
            time_end = time_end.item()
            if isinstance(time_end, Timestamp):
                time_end = time_end.to_pydatetime()
            elif isinstance(time_end, str):
                time_end = parse_datetime(time_end)
        elif isinstance(time_end, datetime64):
            time_end = convert_datetime(time_end)
    else:
        raise ValueError(f"Unsupported indexer type '{type(time_)}'")
    return time_start, time_end


def is_timezone_aware(datetime_obj):
    if datetime_obj.tzinfo is not None:
        if datetime_obj.tzinfo.utcoffset(datetime_obj) is not None:
            return True
        else:
            return False
    else:
        return False


def make_timezone_aware(datetime_obj):
    if not is_timezone_aware(datetime_obj):
        return datetime_obj.replace(tzinfo=timezone.utc)
    else:
        return datetime_obj


def parse_datetime(datetime_str):
    formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"No valid datetime format found. Tried: {', '.join(formats)}")
