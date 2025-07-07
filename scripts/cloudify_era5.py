import intake
from cloudify.utils.datasethelper import *


def add_era5(mapper_dict: dict, dsdict: dict) -> (dict, dict):
    source_catalog = (
        "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/observations/ERA5/new.yaml"
    )
    cat = intake.open_catalog(source_catalog)
    dsone = cat["surface_analysis_monthly"](chunks=None).to_dask().reset_coords()[["lat", "lon"]]
    print(dsone)
    dsone = dsone.load()
    dsnames=[]
    for mdsid in list(cat):
        if "hourly" in mdsid and not "surface" in mdsid:
            continue
        print(mdsid)
        dsnames.append(mdsid)
    rawdsdict=get_dataset_dict_from_intake(cat, dsnames, drop_vars=["lat","lon"])
    for dsname in rawdsdict.keys():
        ds=rawdsdict[dsname]
        for l in ["lat", "lon"]:
            ds.coords[l] = dsone[l].copy()
        mapper_dict, ds = reset_encoding_get_mapper(
            mapper_dict, dsname, ds, desc=cat[dsname].describe()
        )
        ds = gribscan_to_float(ds)
        ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
        ds = set_compression(ds)
        dsdict["era5-dkrz."+dsname] = ds
    del rawdsdict
    return mapper_dict, dsdict
