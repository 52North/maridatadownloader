from maridatadownloader.cds import DownloaderCdsApiERA5
from maridatadownloader.opendap import DownloaderOpendapCMEMS, DownloaderOpendapGFS, DownloaderOpendapETOPONCEI
from maridatadownloader.copernicus_marine_toolbox import DownloaderCopernicusMarineToolboxApi


class DownloaderFactory:
    def __init__(self):
        pass

    @classmethod
    def get_downloader(cls, downloader_type, platform=None, username=None, password=None, **kwargs):

        if downloader_type.lower() == 'opendap':
            if platform.lower() == 'gfs':
                return DownloaderOpendapGFS(username=username, password=password, **kwargs)
            elif platform.lower() == 'cmems':
                assert 'product' in kwargs, "kwargs['product'] is required for platform=cmems"
                assert 'product_type' in kwargs, "kwargs['product_type'] is required for platform=cmems"
                assert username, "username is required for platform=cmems"
                assert password, "password is required for platform=cmems"
                return DownloaderOpendapCMEMS(kwargs.pop('product'), kwargs.pop('product_type'), username, password,
                                              **kwargs)
            elif platform.lower() == 'etoponcei':
                return DownloaderOpendapETOPONCEI(**kwargs)
            else:
                raise ValueError(platform)
        elif downloader_type.lower() == 'cdsapi':
            if platform.lower() == 'era5':
                return DownloaderCdsApiERA5(uuid=username, api_key=password, **kwargs)
        elif downloader_type.lower() == 'cmtapi':
            if platform.lower() == 'cmems':
                return DownloaderCopernicusMarineToolboxApi(username=username, password=password, **kwargs)
            else:
                raise ValueError(platform)
        else:
            raise ValueError(downloader_type)
