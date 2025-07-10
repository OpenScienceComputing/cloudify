import intake
from typing import Dict, Any, List, Optional
from cloudify.utils.datasethelper import (
    get_dataset_dict_from_intake,
    find_data_sources,
    reset_encoding_get_mapper,
    adapt_for_zarr_plugin_and_stac,
    apply_lossy_compression,
    set_compression,
    set_or_delete_coords,
    gribscan_to_float
)
import xarray as xr


def get_dsone(
    cat: intake.Catalog,
    model: str,
    realm: str,
    coords: List[str]
) -> Optional[xr.Dataset]:
    """
    Get coordinate dataset for a specific model and realm.

    This function attempts to load coordinate information from various dataset
    variants for a given model and realm.

    Args:
        cat: Intake catalog
        model: Model identifier
        realm: Model realm (e.g., 'atmos', 'ocean')
        coords: List of coordinate variables to extract

    Returns:
        Optional[xr.Dataset]: Coordinate dataset or None if not found
    """
    for dsname in ["2d_monthly_mean", "monthly_node", "2D_monthly_avg"]:
        try:
            dscoords = (
                cat[f"model-output.{model}.{realm}.native.{dsname}"](chunks=None)
                .to_dask()
                .reset_coords()[coords]
                .chunk()
            )
            return dscoords
        except Exception as e:
            print(f"Failed to load coordinates for {model}.{realm}: {str(e)}")
    return None


def add_eerie(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset]
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add EERIE datasets to the mapper dictionary and dataset dictionary.

    This function processes EERIE model output datasets from the DKRZ intake catalog,
    handling coordinate transformations and dataset preparation for Zarr storage.

    Args:
        mapper_dict: Dictionary mapping dataset IDs to storage mappers
        dsdict: Dictionary mapping dataset IDs to xarray Datasets

    Returns:
        tuple[Dict[str, Any], Dict[str, xr.Dataset]]: Updated mapper_dict and dsdict

    Raises:
        ValueError: If required source catalog is not accessible
    """
    l_dask=True
    # EERIE catalog path
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
    
    try:
        cat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open EERIE catalog: {str(e)}")

    # Define model coordinate datasets
    dsonedict = {
        "icon_atmos": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "atmos",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "icon_ocean": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "ocean",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "icon_land": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "atmos",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "ifs-fesom_atmos": get_dsone(
            cat, "ifs-fesom2-sr.hist-1950.v20240304", "atmos", ["lat", "lon"]
        ),
        "ifs-fesom_ocean": get_dsone(
            cat,
            "ifs-fesom2-sr.eerie-spinup-1950.v20240304",
            "ocean",
            ["lat", "lon", "coast"],
        ),
        "ifs-amip-tco1279_atmos": get_dsone(
            cat, "ifs-fesom2-sr.hist-1950.v20240304", "atmos", ["lat", "lon"]
        ),
    }

    # Find datasets to process
    hostids = []
    for source in list(cat["model-output"]):
        if source != "csv" and source != "esm-json":
            hostids += [
                    a
                    for a in find_data_sources(cat["model-output"][source])
                    if not any(b in a for b in ["v2023", "3d_grid"])
            ]

    localdsdict = get_dataset_dict_from_intake(
        cat["model-output"],
        hostids,
        whitelist_paths=["bm1344", "bm1235", "bk1377"],
        storage_chunk_patterns=["native", "grid", "healpix", ".mon"],
        drop_vars=dict(
            native=["lat", "lon", "cell_sea_land_mask"],
            healpix=["lat", "lon"]
        ),
    )

    for dsid, ds in localdsdict.items():
        ds.attrs["_catalog_id"] = dsid
        if "native" in dsid:
            dsone = dsonedict.get(
                next(
                    (
                        k
                        for k in dsonedict.keys()
                        if all(b in dsid for b in k.split("_"))
                    ), "nothing_found"
                ),
                "nothing_found"
            )
        if dsone and not "elem" in dsid:
            try:
                for dv in dsone.variables.keys():
                    ds.coords[dv] = dsone[dv]
            except:
                print(f"Couldnt set coordinates for {dsid}")
        else:
            print(f"Couldnt find coords for {dsid}")
        if "d" in ds.data_vars:
            ds = ds.rename(d="testd")

        if "fesom" in dsid:  #            and "ocean" in dsid and "native" in dsid:
            if not l_dask:
                continue
            # ds = chunk_and_prepare_fesom(ds)
            if "heightAboveGround" in ds.variables:
                ds = ds.drop("heightAboveGround")

        if "hadgem" in dsid:
            droplist = [a for a in ["height"] if a in ds.variables]
            if droplist:
                ds = ds.drop(droplist)
        ds = set_or_delete_coords(ds, dsid)
        ds = gribscan_to_float(ds)

        # lossy has to come first!
        if not "grid" in dsid:
            if "native" in dsid or "_11" in dsid or "_10" in dsid:
                ds = apply_lossy_compression(ds, l_dask)
        mapper_dict, ds = reset_encoding_get_mapper(
            mapper_dict,
            dsid,
            ds,  # desc=desc
        )
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
        ds = set_compression(ds)
        dsdict[dsid] = ds
    del localdsdict

    return mapper_dict, dsdict
