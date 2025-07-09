from typing import Dict, Any
import glob
from tqdm import tqdm
import xarray as xr
from cloudify.utils.datasethelper import (
    reset_encoding_get_mapper,
    adapt_for_zarr_plugin_and_stac,
    set_compression
)

# Configuration constants
TRUNK = "/work/mh0492/m301067/orcestra/healpix/"
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
    dsdict: Dict[str, xr.Dataset]
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
        for a in sorted(glob.glob(TRUNK + "/*"))
        if a.split("/")[-1][0] == "0"
        if not "-rerun" in a
    ]
    dsone = None
    for ini in tqdm(init_dates_trunks):
        init_date = ini.split("/")[-1]
        for dim in DIMS:
            dstrunk = f'{ini}/{conf_dict["experiment_id"]}_{init_date}_{dim}_hpz12.zarr'
            dsname = f'{conf_dict["project_id"].lower()}.{conf_dict["source_id"]}.s2024-{init_date[0:2]}-{init_date[2:4]}_{dim}_PT10M_12'
            if dim == "3d":
                dsname = dsname.replace("PT10M", "PT4H")
            ds = xr.open_dataset(
                dstrunk, engine="zarr", consolidated=True, chunks="auto"
            )
            if not dsone:
                dsone = ds.copy()
            ds["cell"] = dsone["cell"]
            ds.attrs.update(conf_dict)
            print(ds.encoding["source"])
            print(dsname)
            mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsname, ds)
            ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
            ds = set_compression(ds)
            dsdict[dsname] = ds
    return mapper_dict, dsdict
