import intake
from datasethelper import *


def add_era5(mapper_dict: dict, dsdict: dict) -> (dict, dict):
    source_catalog = (
        "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/observations/ERA5/new.yaml"
    )
    cat = intake.open_catalog(source_catalog)
    dsone = cat[list(cat)[1]].to_dask().reset_coords()[["lat", "lon"]]
    print(dsone)
    dsone = dsone.load()
    for mdsid in list(cat):
        dsname = "era5-dkrz." + mdsid
        desc = cat[mdsid].describe()
        if "hourly" in mdsid and not "surface" in mdsid:
            continue
        print(dsname)
        #        try:
        if True:
            ds = cat[mdsid].to_dask()
            for l in ["lat", "lon"]:
                if l in ds.variables:
                    del ds[l]
                    ds[l] = dsone[l].copy()
                    ds = ds.set_coords(l)
            mapper_dict, ds = reset_encoding_get_mapper(
                mapper_dict, dsname, ds, desc=desc
            )
            ds = gribscan_to_float(ds)
            ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
            ds = set_compression(ds)
        #        except Exception as e:
        #            print(e)
        #            continue
        dsdict[dsname] = ds
    return mapper_dict, dsdict
