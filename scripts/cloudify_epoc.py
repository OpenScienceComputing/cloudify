from typing import Dict, Any, List, Optional
from cloudify.utils.datasethelper import (
    get_dataset_dict_from_intake,
    find_data_sources_v2,
    reset_encoding_get_mapper,
    adapt_for_zarr_plugin_and_stac,
    apply_lossy_compression,
    set_compression,
    set_or_delete_coords,
    dotted_get
)
import intake
import xarray as xr
from cloudify.utils.statistics import (
    build_summary_df,
    summarize_overall,
    print_summary    
)

def get_dsone(
    cat: intake.Catalog,
    model: str,
    realm: str,
    coords: List[str],
) -> Optional[xr.Dataset]:
    for dsname in ["2d_daily_mean"]:
        dscoords = None
        try:
        #if True:
            dscoords = (
                dotted_get(cat,f"model-output.{model}.{realm}.native.{dsname}")(chunks=None)
                .read()
                .reset_coords()[coords]
                .chunk()
            )
        except Exception as e:
            continue
        return dscoords
    print(f"Failed to load coordinates for {model}.{realm}")
    return None

def add_epoc(
    mapper_dict: Dict[str, Any],
    dsdict: Dict[str, xr.Dataset],
    l_dask: bool = True
) -> tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
    """
    Add EPOC datasets to the mapper dictionary and dataset dictionary.

    This function processes EPOC datasets from the DKRZ catalog,
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
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_epoc/disk/main2.yaml"
    try:
        cat = intake.open_catalog(source_catalog)
    except Exception as e:
        raise ValueError(f"Failed to open EPOC catalog: {str(e)}")

    if l_dask:    
        dsonedict = {
            "icon_atmos": get_dsone(
                cat,
                "icon-epoc.control-1990.v20250325",
                "atmos",
                ["lat", "lon", "cell_sea_land_mask"],
            ),
            "icon_ocean": get_dsone(
                cat,
                "icon-epoc.control-1990.v20250325",
                "ocean",
                ["lat", "lon", "cell_sea_land_mask"],
            ),
            "icon_land": get_dsone(
                cat,
                "icon-epoc.control-1990.v20250325",
                "atmos",
                ["lat", "lon", "cell_sea_land_mask"],
            )
        }
    hostids = []
    for source in cat["model-output"].read().entries:
        print(source)
        hostids += [
            a
            for a in find_data_sources_v2(cat["model-output"].read()[source].read(),name=source)
            if not "model-level" in a
            #if not any(b in a for b in ["v2023", "3d_grid"]) and not "spinup" in a #and "ifs-amip" in a 
        ]

    print(hostids)
    localmapperdict, localdsdict = get_dataset_dict_from_intake(
        cat["model-output"].read(),
        hostids,
        drop_vars=dict(
            native=["lat", "lon", "cell_sea_land_mask"],
        ),
        l_dask=l_dask,
        cache_size=0
    )
        
    df=build_summary_df(localdsdict)
    df.to_csv("/tmp/epoc_datasets.csv")
    su=summarize_overall(df)
    print(print_summary(su))    
    
    for dsid, ds in localdsdict.items():
        ds.attrs["_catalog_id"] = dsid
        urlpath = ds.encoding["source"]
        try:
            mapper_dict[urlpath] = localmapperdict.pop(urlpath)
        except:
            continue
        if l_dask:
            dsone = None
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
                    print("Start setting grid")
                    try:
                        for dv in dsone.variables.keys():
                            ds.coords[dv] = dsone[dv]
                    except:
                        print(f"Couldnt set coordinates for {dsid}")
                else:
                    print(f"Couldnt find coords for {dsid}")

        ds = set_or_delete_coords(ds, dsid)
        if l_dask:
            ds = ds.drop_encoding()            
            ds = apply_lossy_compression(ds, l_dask)
            ds.encoding["source"]=urlpath
            
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
        ds = set_compression(ds)
        dsdict[dsid] = ds
    del localdsdict
    return mapper_dict, dsdict
