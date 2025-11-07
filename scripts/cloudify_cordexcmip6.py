from typing import Dict, Any
import glob
from tqdm import tqdm
import xarray as xr
from cloudify.utils.datasethelper import (
    #reset_encoding_get_mapper,
    open_zarr_and_mapper,
    adapt_for_zarr_plugin_and_stac,
    set_compression,
    
)
from cloudify.utils.statistics import (
    build_summary_df,
    summarize_overall,
    print_summary
)

import os
TRUNK="/work/bm1344/DKRZ/kerchunks_batched/CORDEX-CMIP6"

def add_cordexcmip6(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset],
    l_dask: bool = True
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:


    parquet_dirs=[]

    print("starting glob")
    for dirpath, dirnames, filenames in os.walk(TRUNK):
        for dirname in dirnames:
            if dirname.endswith(".parq"):
                parquet_dirs.append(os.path.join(dirpath, dirname))
    
    dsone = None
    local_dsdict={}
    for ini in tqdm(parquet_dirs):
        dsname='cordex-cmip6.'+'.'.join('.'.join(ini.split('/')[6:]).split('.')[:-1])
        print(dsname)
        #if not "0819" in ini:
        #    continue
        chunks="auto"
        if not l_dask:
            chunks=None
        opts=dict(
            consolidated=False,
            chunks=chunks,
        )                
        ds, mapper_dict["reference::/"+ini] = open_zarr_and_mapper(
                "reference::/"+ini, 
                storage_options=dict(cache_size=0,lazy=True,remote_protocol="file"),
                **opts
                )
        print(dsname)
            #mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsname, ds, l_dask=l_dask)
        ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
        ds = set_compression(ds)
        ds.encoding["source"]="reference::/"+ini
        dsdict[dsname] = ds
        local_dsdict[dsname] = ds
            

    df=build_summary_df(local_dsdict)
    df.to_csv("/tmp/cordexcmip6_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))
    del local_dsdict
            
    return mapper_dict, dsdict
