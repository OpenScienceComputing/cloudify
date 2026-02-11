from typing import Dict, Any
import glob
from pathlib import Path
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

# Configuration constants
#TRUNK = "/work/mh0492/m301067/orcestra/healpix/"
TRUNK = "/work/mh0066/ICON-LAM"
DIMS = ["2d", "3d"]

# ORCESTRA dataset configuration
conf_dict = dict(
    source_id="ICON-LAM",
    institution_id="MPI-M",
    project_id="ORCESTRA",
    activity_id="ORCESTRA",
    experiment_id="orcestra_1250m",
    authors="Romain Fiévet",
    contact="romain.fievetATmpimet.mpg.de",
    description="https://orcestra-campaign.org/lam.html",
)


def add_orcestra(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset],
    l_dask: bool = True
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add ORCESTRA datasets to the mapper dictionary and dataset dictionary.

    This function processes ORCESTRA healpix datasets from the specified trunk directory,
    handling both 2D and 3D dimensions, and preparing them for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required trunk directory or datasets are not accessible
    """
    # Find all initial date directories
    init_dates_trunks = [
        a
        for a in sorted(glob.glob(TRUNK + "/*.zarr"))
    ]
    local_dsdict={}
    for ini in tqdm(init_dates_trunks):
        print(ini)
        dsname = '.'.join(ini.split('/')[-1].split('.')[:-1])
        chunks="auto"
        if not l_dask:
            chunks=None
        ds, mapper_dict[ini] = open_zarr_and_mapper(
            ini, storage_options=dict(cache_size=0),chunks=chunks
        )
        ds.attrs.update(conf_dict)
        ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
        ds.encoding["source"]=ini
        dsdict[dsname] = ds
        local_dsdict[dsname] = ds

    df=build_summary_df(local_dsdict)
    df.to_csv("/tmp/orcestra_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))
    del local_dsdict
            
    return mapper_dict, dsdict
