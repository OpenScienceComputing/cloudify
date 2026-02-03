from hsmspec import HSMFileSystem
import fsspec
fsspec.register_implementation("hsm", HSMFileSystem)
import xarray as xr
from typing import Dict, Any, Optional
from cloudify.utils.datasethelper import (
    open_zarr_and_mapper,
)

def add_tape(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset],
    l_dask: bool = False,
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:

    urlpath="reference:://work/bm1344/DKRZ/kerchunks_pp_batched/ICON/hist-1950/v20240618/atmos_native_2d_1h_inst_hsm.parq"
    dsdict["tape_test"], mapper_dict[urlpath] = open_zarr_and_mapper(
            urlpath,
            storage_options=dict(
                remote_protocol="hsm",
                remote_options=dict(
                    DISK_CACHE="/scratch/k/k202134/INTAKE_CACHE",
                    asyncronous=True
                    )
                ),
            chunks="auto",
            consolidated=False
            )
    dsdict["tape_test"].attrs["from_tape"]="Yes"
    dsdict["tape_test"].encoding["source"]=urlpath
    return mapper_dict, dsdict    
