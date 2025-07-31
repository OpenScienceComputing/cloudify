from typing import Dict, Any, Optional
from cloudify.utils.datasethelper import reset_encoding_get_mapper, adapt_for_zarr_plugin_and_stac
import intake
import xarray as xr
from cloudify.utils.statistics import (
    build_summary_df,
    summarize_overall,
    print_summary
)

def add_cosmorea(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset],
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add COSMO Reanalysis datasets to the mapper dictionary and dataset dictionary.

    This function processes COSMO Reanalysis datasets from the DKRZ Swift catalog,
    handling coordinate transformations and dataset preparation for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets
        L_DASK: Whether to use Dask for lazy loading (default: True)

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required source catalog is not accessible
    """
    # COSMO Reanalysis catalog URL
    source_catalog = "https://swift.dkrz.de/v1/dkrz_4236b71e-04df-456b-8a32-5d66641510f2/catalogs/cosmo-rea/main.yaml"
    l_dask = False
    try:
        cat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open COSMO Reanalysis catalog: {str(e)}")

    # Define dimensions and variables to process
    onedims = ["height", "rotated_latitude_longitude"]
    drop_vars = [
        "b_bnds",
        "lev_bnds",
        "plev_bnds",
        "rlat_bnds",
        "rlon_bnds",
        "vertices_latitude",
        "vertices_longitude",
        "blub",
    ]

    # Load base coordinates
    if l_dask:
        try:
            dsone = (
                cat["mon_atmos"](chunks=None)
                .to_dask()
                .reset_coords()[["latitude", "longitude"]]
            )
            dsone = dsone.load()
        except Exception as e:
            raise ValueError(f"Failed to load base coordinates: {str(e)}")

    # Process each dataset in the catalog
    local_dsdict={}
    for dsname in list(cat):
        print(dsname)
        chunks="auto"
        if not l_dask:
            chunks=None
        ds = cat[dsname](drop_variables=drop_vars,chunks=chunks).to_dask()
        for onedim in onedims:
            if onedim in ds.variables and "time" in ds[onedim].dims:
                ds[onedim] = ds.reset_coords()[onedim].isel(time=0).load()
        if l_dask:
            for l in ["latitude", "longitude"]:
                ds.coords[l] = dsone[l]
        desc = cat[dsname].describe()
        desc["args"]["storage_options"]["remote_protocol"] = "file"
        dsid = "cosmo-rea-" + dsname
        mapper_dict, ds = reset_encoding_get_mapper(
            mapper_dict, dsid, ds, desc=desc, l_dask=l_dask
        )
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
        dsdict[dsid] = ds
        local_dsdict[dsid] = ds
        
    df=build_summary_df(local_dsdict)
    df.to_csv("cosmo_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))
    del local_dsdict
    return mapper_dict, dsdict
