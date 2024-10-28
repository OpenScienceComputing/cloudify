import os
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
import xarray as xr
import xpublish as xp
import asyncio
import nest_asyncio
nest_asyncio.apply()
import glob
trunk="/work/bb1149/ESGF_Buff/"
dset="CORDEX/CMIP6/DD/EUR-12/CLMcom-DWD/ERA5/evaluation/r1i1p1f1/ICON-CLM-202407-1-1/v0-r0/1hr/*/v20240713/*"
dset="CORDEX/CMIP6/DD/EUR-12/CLMcom-DWD/ERA5/evaluation/r1i1p1f1/ICON-CLM-202407-1-1/v0-r0/mon/*/v20240713/*"
testvar="ts"
fn=sorted(glob.glob(trunk+dset.replace("/*/","/ts/")))[-1]
timestamp=fn.split('/')[-1].split('_')[9]
lastyear=timestamp[0:4]
import pandas as pd

def sortout(dss,cm):
    l_iscm=any(
            cm in dss[dv].attrs.get("cell_methods","default")
            for dv in dss.data_vars
            )
    if l_iscm:
        return dss
    else:
        dss=dss.isel(time=0)
        dss["time"]=dss['time'] - pd.Timedelta(minutes=30)
        return dss

if __name__ == "__main__":  # This avoids infinite subprocess creation
    #client = asyncio.get_event_loop().run_until_complete(get_dask_client())
    import dask
    dask.config.set({"array.slicing.split_large_chunks": False})
    dask.config.set({"array.chunk-size": "100 MB"})
    zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    #client=Client(cluster)
    os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address
    dsdict={}
    template=trunk+dset+str(lastyear)+"*"
    if "/1hr" in dset:
        for freq,cm in [("1hr","time: mean"),("1hrPt","time: point")]:
            ds=xr.open_mfdataset(
                template,
                compat="override",
                coords="minimal",
                chunks="auto",
                preprocess=lambda dss: sortout(dss,cm)
                )
            del ds["time_bnds"]
            entry=dset.replace('1hr/*',freq).replace('/*','').replace('/','.')
            print(entry)
            dsdict[entry]=ds.chunk(time=1)
    else:
        ds=xr.open_mfdataset(
                template,
                compat="override",
                coords="minimal",
                chunks="auto",
                )
        del ds["time_bnds"]
        entry=dset.replace('/*','').replace('/','.')
        print(entry)
        dsdict[entry]=ds.chunk(time=1)        

    collection = xp.Rest(dsdict,cache_kws=dict(available_bytes=1000000000))
    collection.register_plugin(PlotPlugin())
    collection.serve(host="0.0.0.0", port=9000)

