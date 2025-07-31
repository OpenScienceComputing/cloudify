from typing import Dict, Any, Optional
from cloudify.utils.datasethelper import (
    reset_encoding_get_mapper,
    adapt_for_zarr_plugin_and_stac,
    set_compression,
    get_dataset_dict_from_intake,
    gribscan_to_float,
    apply_lossy_compression
)
import xarray as xr
import intake
import yaml
import fsspec
from copy import deepcopy as copy
from cloudify.utils.statistics import (
    build_summary_df,
    summarize_overall,
    print_summary
)


def refine_nextgems(
    mapper_dict: Dict[str, Any],
    iakey: str,
    ds: xr.Dataset,
    desc: Dict[str, Any],
    l_dask: bool = True
) -> tuple[Dict[str, Any], xr.Dataset]:
    """
    Refine NEXTGEMS dataset with metadata and compression settings.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        iakey: Dataset identifier
        ds: xarray Dataset to refine
        desc: Dataset description containing metadata

    Returns:
        tuple[Dict[str, Any], xr.Dataset]: Updated mapper_dict and refined dataset
    """
    # Add metadata from description
    for mdk, mdv in desc.get("metadata", {}).items():
        if mdk not in ds.attrs:
            ds.attrs[mdk] = mdv

    # Prepare dataset for storage
    mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, iakey, ds, desc=desc, l_dask=l_dask)
    ds = adapt_for_zarr_plugin_and_stac(iakey, ds)
    ds = set_compression(ds)

    return mapper_dict, ds


def get_args(desc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get processed arguments from dataset description.

    Args:
        desc: Dataset description containing arguments

    Returns:
        Dict[str, Any]: Processed arguments with updated storage options
    """
    args = copy(desc["args"])
    if not args.get("storage_options"):
        args["storage_options"] = {}

    # Handle reference paths
    if isinstance(args["urlpath"], str) and args["urlpath"].startswith("reference"):
        if not args["storage_options"].get("remote_protocol"):
            args["storage_options"].update(dict(lazy=True, remote_protocol="file"))

    # Set chunking strategy
    args["chunks"] = "auto"
    del args["urlpath"]

    return args


def add_healpix(
    i: str,
    v: xr.Dataset
) -> xr.Dataset:
    """
    Add healpix grid mapping information to dataset.

    Args:
        i: Dataset identifier containing healpix level information
        v: xarray Dataset to modify

    Returns:
        xr.Dataset: Dataset with added healpix grid mapping
    """
    try:
        # Load CRS information
        crs = xr.open_zarr(
            "/work/bm1235/k202186/dy3ha-rechunked/d3hp003.zarr/PT1H_inst_z0_atm"
        )["crs"]

        # Extract healpix level from identifier
        levstr = i.split("healpix")[1].split(".")[0].split("_")[0]
        lev = int(levstr)

        # Add healpix information
        v["crs"] = crs["crs"]
        v["crs"].attrs["healpix_nside"] = lev
        for dv in v.data_vars:
            v[dv].attrs["grid_mapping"] = "crs"

        return v

    except Exception as e:
        print(f"Warning: Failed to add healpix information: {str(e)}")
        print(f"Identifier: {i}")
        return v


def add_nextgems(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset]
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add NEXTGEMS datasets to the mapper dictionary and dataset dictionary.

    This function processes NEXTGEMS datasets from the DKRZ intake catalog,
    handling coordinate transformations, healpix grid mapping, and dataset
    preparation for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required source catalogs are not accessible
    """
    # NEXTGEMS catalog paths
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_nextgems/catalog.yaml"
    published_catalog = "https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml"
    
    # Define datasets to process
    DS_ADD = [
        "ICON.ngc5004"
    ]  # "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512"]
    DS_ADD_SIM = [
        "IFS.IFS_2.8-FESOM_5-production-parq",
        "IFS.IFS_2.8-FESOM_5-production-deep-off-parq",
    ]

    l_dask=False
    try:
        ngccat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open NEXTGEMS catalog: {str(e)}")

    try:
        prodcat = intake.open_catalog(published_catalog)
        ngc4_md = yaml.safe_load(prodcat.yaml())["sources"]["nextGEMS_prod"]["metadata"]
    except Exception as e:
        print(f"Warning: Failed to load catalogs: {str(e)}")
        ngc4_md = None

    if l_dask:
        gr_025 = (
            ngccat["IFS.IFS_2.8-FESOM_5-production-parq"]["2D_monthly_0.25deg"]
            .to_dask()
            .reset_coords()[["lat", "lon"]]
            .load()
        )

    # Build list of all datasets to process
    all_ds = copy(DS_ADD)
    for sim in DS_ADD_SIM:
        for dsn in list(ngccat[sim]):
            all_ds.append(sim + "." + dsn)

    # Get dataset descriptions
    descdict = {}

    localdsdict = get_dataset_dict_from_intake(
        ngccat,
        all_ds,
        prefix="nextgems.",
        storage_chunk_patterns=["2048"],
        drop_vars={"25deg": ["lat", "lon"]},
        l_dask=False
    )

    # Process each dataset
    for dsn in list(localdsdict.keys()):
        iakey = dsn.replace("-parq", "")
        desckey = iakey.replace("nextgems.", "")
        localdsdict[iakey] = localdsdict.pop(dsn)

        try:
            descdict[iakey] = ngccat[desckey].describe()
        except:
            descdict[iakey] = ngccat[".".join(desckey.split(".")[:-1])].describe()
        if "ngc4008" in dsn or "IFS_9-FESOM_5-production" in dsn:
            descdict[iakey]["metadata"] = ngc4_md

    df=build_summary_df(localdsdict)
    df.to_csv("nextgems_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))

    for ia, ds in localdsdict.items():
        # for ia in all_ds:
        print(ia)
        if l_dask:
            print("gribscan to float")
            ds = gribscan_to_float(ds)
        mapper_dict, ds = refine_nextgems(mapper_dict, ia, ds, descdict[ia],l_dask=l_dask)
        if l_dask:
            if "25deg" in ia :
                ds.coords["lat"] = gr_025["lat"]
                ds.coords["lon"] = gr_025["lon"]
            print("try to set crs")
            if "crs" in ds.variables:
                if len(str(ds["crs"].attrs.get("healpix_nside", "No"))) >= 4:
                    ds = apply_lossy_compression(ds)
        elif "healpix" in ia:
            ds = add_healpix(ia, ds)
        dsdict[ia] = ds
    del localdsdict
    return mapper_dict, dsdict
