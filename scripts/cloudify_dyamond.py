from typing import Dict, Any
from cloudify.utils.datasethelper import (
    get_dataset_dict_from_intake,
    reset_encoding_get_mapper,
    adapt_for_zarr_plugin_and_stac,
    set_compression
)
from cloudify.utils.statistics import (
    build_summary_df,
    summarize_overall,
    print_summary
)

import intake
import xarray as xr

def add_dyamond(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset]
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add DYAMOND datasets to the mapper dictionary and dataset dictionary.

    This function processes DYAMOND datasets from the HK25 intake catalog,
    handling specific dataset configurations and preparation for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required source catalog is not accessible
    """
    # DYAMOND catalog path
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_hk25/catalog.yaml"
    l_dask=False
    try:
        cat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open DYAMOND catalog: {str(e)}")

    # Define datasets to process
    DS_ADD = ["EU.icon_d3hp003"]

    # Load datasets with specific configurations
    localdsdict = get_dataset_dict_from_intake(
        cat,
        DS_ADD,
        prefix="dyamond",
        storage_chunk_patterns=["EU"],
        allowed_ups=dict(zoom=["10", "11"]),
        mdupdate=True,
        l_dask=l_dask
    )
    df=build_summary_df(localdsdict)
    df.to_csv("dyamond_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))

    # Process each dataset
    for dsn, ds in localdsdict.items():
        mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsn, ds, l_dask=l_dask)
        ds = adapt_for_zarr_plugin_and_stac(dsn, ds)
        ds = set_compression(ds)
        dsdict[dsn] = ds

    del localdsdict
    
    return mapper_dict, dsdict
