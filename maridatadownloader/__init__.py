from maridatadownloader.cds import DownloaderCdsapiERA5
from maridatadownloader.opendap import DownloaderOpendapCMEMS, DownloaderOpendapGFS, DownloaderOpendapETOPONCEI
from maridatadownloader.copernicus_marine_toolbox import DownloaderCopernicusMarineToolboxapi

class DownloaderFactory:
    def __init__(self):
        pass

    @classmethod
    def get_downloader(cls, downloader_type, platform=None, username=None, password=None,**kwargs):
        if downloader_type.lower() == 'opendap':
            if platform.lower() == 'gfs':
                return DownloaderOpendapGFS(username=username, password=password)
            elif platform.lower() == 'cmems':
                assert 'product' in kwargs, "kwargs['product'] is required for platform=cmems"
                assert 'product_type' in kwargs, "kwargs['product_type'] is required for platform=cmems"
                assert username, "username is required for platform=cmems"
                assert password, "password is required for platform=cmems"
                return DownloaderOpendapCMEMS(kwargs['product'], kwargs['product_type'], username, password)
            elif platform.lower() == 'etoponcei':
                return DownloaderOpendapETOPONCEI()
            else:
                raise ValueError(platform)
        elif downloader_type.lower() == 'cdsapi':
            if platform.lower() == 'era5':
                return DownloaderCdsapiERA5(uuid=username, api_key=password)
        elif downloader_type.lower() == 'cmtapi':
            return DownloaderCopernicusMarineToolboxapi(cmems_username=username, cmems_password=password)
        else:
            raise ValueError(downloader_type)