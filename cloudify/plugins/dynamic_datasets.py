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
            self.url_split.join(
                str(path.name).split(self.url_split)[2:]
                )
            for path in pathlib.Path(self.source).iterdir()
        ]

    @hookimpl
    def get_dataset(self, dataset_id: str):
        inputs = self.get_datasets()
        if dataset_id not in inputs:
            raise HTTPException(status_code=404, detail=f"Could not find {dataset_id}")
        
        input_urls = [
                a
                for a in pathlib.Path(self.source).iterdir()
                if str(a.name).endswith(dataset_id)
                ]
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
            input_url = str(input_url.name)
            port = input_url.split(self.url_split)[1]
            protocol = "http://"
            if not port.isdigit():
                raise HTTPException(status_code=404, detail=f"Port needs to be integer {port}")
            elif int(port) == 443:
                protocol = "https://"
            input_url = (
                    protocol+input_url.split(self.url_split)[0]+":"+port+"/datasets/"+
                    self.url_split.join(input_url.split(self.url_split)[2:])+
                    "/kerchunk"
            )
            print(input_url)
            try:
                ds = xr.open_dataset(
                    input_url,
                    chunks="auto",
                    engine="zarr"
                    )
                ds = ds.drop_encoding()
                ds.encoding["source"] = input_url
                return ds
            except:
                raise HTTPException(status_code=404, detail=f"Could not open {input_url}")
