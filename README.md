## maridatadownloader

Download tools for metocean (dynamic meteorological and oceanographic conditions) and static bathymetric data.

Currently supported platforms/provider:
 - Global Forecast System (GFS)
 - Copernicus Marine Environment Monitoring Service (CMEMS)
 - ECMWF ERA5 reanalysis from Copernicus Climate Data Store (CDS)
 - ETOPO Global Relief Model (NOAA/NCEI)

A detailed list of available downloaders and datasets can be found in the next section.

### Available datasets/downloaders

| Downloader name | Platform/Provider | Access service/API         | Type of data         | Dataset id                                  | References |
|-----------------|-------------------|----------------------------|----------------------|---------------------------------------------|------------|
| cmems¹          | CMEMS             | Copernicus Marine Toolbox  | Ocean waves          | cmems_mod_glo_wav_anfc_0.083deg_PT3H-i²     | [1]        |
| cmems¹          | CMEMS             | Copernicus Marine Toolbox  | Ocean currents       | cmems_mod_glo_phy_anfc_merged-uv_PT1H-i²    | [2]        |
| cmems¹          | CMEMS             | Copernicus Marine Toolbox  | Ocean physics        | cmems_mod_glo_phy_anfc_0.083deg_PT1H-m²     | [2]        |
| gfs             | NOAA/NCEP         | OPeNDAP (via xarray)       | Weather/Atmosphere   | -                                           | [3]        |
| etopo           | NOAA/NCEI         | OPeNDAP (via xarray)       | Topology/Bathymetric | -                                           | [4]        |
| era5¹           | Copernicus CDS    | Copernicus CDS API         | Atmosphere/Ocean     | -                                           | [5]        |

¹Registration needed  
²Check the CMEMS product catalog for additional products: https://data.marine.copernicus.eu/products  

Dataset references:
- [1] https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_WAV_001_027/description
- [2] https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description
- [3] https://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p25deg/catalog.html
- [4] https://www.ncei.noaa.gov/products/etopo-global-relief-model
- [5] https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels

### Installation

```
pip install git+https://github.com/52North/maridatadownloader.git
```

### Usage

#### Basics

Every downloader provides the same two methods:
- `get_xarray_dataset(parameters, subset)` returns the (lazy, if supported by the data source) `xarray.Dataset`
- `save_to_file(path, parameters, subset)` saves the dataset as NetCDF file

```python
from maridatadownloader import BoxSubset, get_downloader

# Create downloader object. Source specific settings (credentials, dataset id, ...) are passed as keyword arguments.
downloader = get_downloader(
    "cmems",
    dataset_id="cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
    username="<username>",
    password="<password>",
)

# Define parameters and subset
parameters = ["VHM0", "VMDR"]
subset = BoxSubset(
    time=slice("2023-11-24T10:30:00", "2023-11-25T10:30:00"),
    latitude=slice(51.5, 52.5),
    longitude=slice(7, 8),
)

xarray_dataset = downloader.get_xarray_dataset(parameters=parameters, subset=subset)
downloader.save_to_file("waves.nc", parameters=parameters, subset=subset)
```

Use `available_downloaders()` to list the names which can be passed to `get_downloader`.

#### Conventions

The datasets returned by all downloaders follow these conventions, independently of the original file/dataset:
- Coordinates are named "time", "latitude" and "longitude"
- Latitude is ascending and defined from -90° to 90°
- Longitude is ascending and defined from -180° to 180°

Subsets always use these names.

#### Sub-setting

The sub-setting logic is implemented using xarray. For a detailed documentation of sub-setting with xarray check the dedicated section on their website: https://docs.xarray.dev/en/latest/user-guide/indexing.html.

**Data cube** (orthogonal indexing) with `BoxSubset`:

```python
from maridatadownloader import BoxSubset

# By value (xarray.Dataset.sel)
BoxSubset(
    time=slice("2023-11-24T10:30:00", "2023-11-25T10:30:00"),
    latitude=slice(51.5, 52.5),
    longitude=7.0,
)
# By value with inexact matches
BoxSubset(latitude=51.53, longitude=[7.02, 7.48], method="nearest")
# By value with interpolation to off-grid values (xarray.Dataset.interp), slices are applied with sel
BoxSubset(
    time=slice("2023-11-24", "2023-11-25"),
    latitude=51.53,
    longitude=7.02,
    interpolate=True,
)
# By index (xarray.Dataset.isel)
BoxSubset(time=0, latitude=slice(0, 10), longitude=slice(0, 10), by="index")
# Additional coordinates
BoxSubset(time="2023-11-24T12:00:00", extra={"height_above_ground": 10})
```

**Trajectory** (vectorized indexing along the dimension 'trajectory') with `TrajectorySubset`:

```python
from datetime import datetime
from maridatadownloader import TrajectorySubset

subset = TrajectorySubset(
    time=[
        datetime(2023, 9, 20, 9),
        datetime(2023, 9, 20, 11),
        datetime(2023, 9, 20, 13),
    ],
    latitude=[51.9, 53.0, 54.0],
    longitude=[2.81, 3.19, 4.56],
    method="linear",  # interpolation method ('linear' or 'nearest')
    extra={"height_above_ground": 10},  # additional coordinates (exact matches)
    fill_nan="linear",  # optional: fill NaN values (e.g. on land pixels) before interpolating
)
# or from a pandas.DataFrame with the columns 'time', 'latitude' and 'longitude'
subset = TrajectorySubset.from_dataframe(
    df_positions, every_nth_row=10, method="linear"
)
```

Before interpolating, the data is reduced to a sub cube covering the trajectory (plus the next grid points and a spatial buffer `buffer_deg`).
With `interpolate=False`, inexact matches (`xarray.Dataset.sel`) are used instead of interpolation.
Times without timezone are interpreted as UTC.

Further reading:
 - https://docs.xarray.dev/en/stable/user-guide/indexing.html#vectorized-indexing
 - https://docs.xarray.dev/en/stable/user-guide/interpolation.html#advanced-interpolation

**Note on the ERA5 downloader**:
The data is not lazy-loaded. Instead, a CDS request is built from the parameters and the time range and bounding box of the subset.
Parameters have to be given as CDS variable names (e.g. `'10m_u_component_of_wind'`), while the returned dataset uses the short names (e.g. `'u10'`).
The API key is the personal access token of your CDS profile.

```python
era5 = get_downloader("era5", api_key="<personal-access-token>")
dataset = era5.get_xarray_dataset(
    ["10m_u_component_of_wind"],
    BoxSubset(
        time=slice("2023-01-01", "2023-01-02"),
        latitude=slice(50, 55),
        longitude=slice(2, 8),
    ),
)
```

#### Chunking

The GFS and ETOPO downloaders can use chunking via dask. The chunk sizes are passed to `xarray.open_dataset`.

```python
chunks = {"lat": 100, "lon": 100}
gfs = get_downloader("gfs", chunks=chunks)
```

Further reading:
 - https://docs.xarray.dev/en/stable/user-guide/dask.html
 - https://examples.dask.org/xarray.html

#### Adding a downloader

Subclass `Downloader`, implement `open_dataset` and optionally override the hooks `select_parameters`, `normalize` (apply the conventions before sub-setting) and `postprocess`.
Register the class with the `register` decorator to make it available via `get_downloader`:

```python
from maridatadownloader import Downloader, register
from maridatadownloader.utils import open_xarray_dataset, rename_if_present


@register("my_source")
class DownloaderMySource(Downloader):
    def open_dataset(self, request):
        if self._dataset is None:
            self._dataset = open_xarray_dataset(
                "https://example.org/thredds/dodsC/my_dataset.nc"
            )
        return self._dataset

    def normalize(self, dataset, request):
        return rename_if_present(dataset, {"lat": "latitude", "lon": "longitude"})
```

The `request` holds the requested parameters and subset (`request.time_bounds()`, `request.bbox()`) and can be used to choose the data source, e.g. as the GFS downloader does for archived data.

### Tests

```
pip install -e .[test]
pytest              # offline tests
pytest -m network   # smoke tests against the remote data sources (ETOPO, GFS)
```

### Funding

|                                                                                           Project/Logo                                                                                            | Description |
|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:| :------------- |
| [<img alt="MariData" align="middle" width="267" height="50" src="https://raw.githubusercontent.com/52North/WeatherRoutingTool/main/docs/_static/maridata_logo.png"/>](https://www.maridata.org/) | MariGeoRoute is funded by the German Federal Ministry of Economic Affairs and Energy (BMWi)[<img alt="BMWi" align="middle" width="144" height="72" src="https://52north.org/delivery/MariData/img/bmwi_logo_en.png" style="float:right"/>](https://www.bmvi.de/) |
|               [<img alt="TwinShip" align="middle" src="https://raw.githubusercontent.com/52North/WeatherRoutingTool/main/docs/_static/twinship_logo.png"/>](https://twin-ship.eu/)                | Co-funded by the European Union’s Horizon Europe programme under grant agreement No. 101192583                                                                                                                                                                                              |