import os
os.environ["LD_LIBRARY_PATH"] = "/work/bm0021/conda-envs/cloudify-test/lib"
import socket
import requests
import xarray as xr
import zarr
print(zarr.__version__)
from tqdm import tqdm

hostname = socket.gethostname()
port = 9000
hosturl=f"http://{hostname}:{port}"
dataset_url='/'.join([hosturl,"datasets"])
all_dataset_names = eval(requests.get(dataset_url).text)
print(len(all_dataset_names))
for dsname in tqdm(all_dataset_names):
    dsurl='/'.join([dataset_url,dsname,"kerchunk"])
    try:
    #if True:
        ds=xr.open_dataset(
            dsurl,
            engine="zarr",
            consolidated=True,
            drop_variables=["lat","lon"]
        )
        data_vars=list(ds.data_vars)
        testds=ds[data_vars[0]]
        testsel=testds.isel(**{
            a:0
            for a in list(testds.dims)
        })
        testsel.load()
    except:
        print(dsurl)

