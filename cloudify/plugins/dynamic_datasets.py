from xpublish import Plugin, hookimpl
import xarray as xr
from fastapi import HTTPException
import pathlib

class DynamicKerchunk(Plugin):
    name: str = "dynamic-kerchunk"
    source: str = "/work/bm1344/DKRZ/km-scale-cloud-inputs/"
    url_scheme: str = ["host","port","dataset"]
    url_split: str = "_"

    @hookimpl
    def get_datasets(self):
        return [
            str(path).split('_')[2]
            for path in pathlib.Path(self.source).iterdir()
        ]

    @hookimpl
    def get_dataset(self, dataset_id: str):
        inputs = self.get_datasets()
        if dataset_id not in inputs:
            raise HTTPException(status_code=404, detail=f"Could not find {dataset_id}")
        
        input_urls = list(pathlib.Path(self.source).glob("*"+dataset_id+"*"))
        if not input_urls:
            raise HTTPException(status_code=404, detail=f"Could not find {dataset_id}")
        elif len(input_urls) > 1:
            raise HTTPException(status_code=404, detail=f"Found multiple {dataset_id}")

        input_url = input_urls[0]

        if pathlib.Path(input_url).is_dir():
            try:
                return xr.open_dataset(
                    input_url,
                    engine="kerchunk",
                    chunks="auto"
                )
            except:
                raise HTTPException(status_code=404, detail=f"Could not open {input_url}")
        else:
            input_url = (
                "http://"+input_url.split(self.url_split)[0]+
                ":"+input_url.split(self.url_split)[1]+
                "/"+input_url.split(self.url_split)[2]+
                "/kerchunk"
            )
            try:
                ds = xr.open_dataset(
                    input_url,
                    chunks=None,
                    engine="zarr"
                    )
                ds.encoding["source"] = input_url
                return ds
            except:
                raise HTTPException(status_code=404, detail=f"Could not open {input_url}")