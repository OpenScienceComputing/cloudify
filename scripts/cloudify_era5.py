import intake
from typing import Dict, Any
from cloudify.utils.datasethelper import (
    get_dataset_dict_from_intake,
    reset_encoding_get_mapper,
    gribscan_to_float,
    adapt_for_zarr_plugin_and_stac,
    set_compression
)
import xarray as xr


def add_era5(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset]
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add ERA5 datasets to the mapper dictionary and dataset dictionary.

    This function processes ERA5 datasets from the DKRZ intake catalog,
    handling coordinate transformations and dataset preparation for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required source catalog is not accessible
    """
    # ERA5 catalog path
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/observations/ERA5/new.yaml"
    
    try:
        cat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open ERA5 catalog: {str(e)}")

    # Load base coordinates
    dsone = (
        cat["surface_analysis_monthly"](chunks=None)
        .to_dask()
        .reset_coords()[["lat", "lon"]]
    )
    print(dsone)
    dsone = dsone.load()
    dsnames = []
    for mdsid in list(cat):
        # Skip hourly datasets that are not surface data
        if "hourly" in mdsid and "surface" not in mdsid:
            continue
        print(f"Processing dataset: {mdsid}")
        dsnames.append(mdsid)
    rawdsdict = get_dataset_dict_from_intake(cat, dsnames, drop_vars=["lat", "lon"])
    for dsname in rawdsdict.keys():
        ds = rawdsdict[dsname]
        
        # Set coordinates
        for l in ["lat", "lon"]:
            ds.coords[l] = dsone[l].copy()

        # Prepare dataset for storage
        mapper_dict, ds = reset_encoding_get_mapper(
            mapper_dict, dsname, ds, desc=cat[dsname].describe()
        )

        # Process dataset
        ds = gribscan_to_float(ds)
        ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
        ds = set_compression(ds)

        # Add to dictionary with ERA5 prefix
        dsdict[f"era5-dkrz.{dsname}"] = ds


    # Clean up
    del rawdsdict
    return mapper_dict, dsdict
