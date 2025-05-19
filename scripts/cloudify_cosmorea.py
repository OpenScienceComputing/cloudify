from datasethelper import *
import intake


def add_cosmorea(mapper_dict, dsdict, L_DASK=True):
    source_catalog = "https://swift.dkrz.de/v1/dkrz_4236b71e-04df-456b-8a32-5d66641510f2/catalogs/cosmo-rea/main.yaml"
    cat = intake.open_catalog(source_catalog)
    onedims = ["height", "rotated_latitude_longitude"]
    coords_patterns = ["bnds", "vertices"]
    dsone = cat[list(cat)[1]].to_dask().reset_coords()[["latitude", "longitude"]]
    print(dsone)
    dsone = dsone.load()
    for dsname in list(cat):
        print(dsname)
        ds = cat[dsname].to_dask()
        for onedim in onedims:
            if onedim in ds.variables and "time" in ds[onedim].dims:
                ds[onedim] = ds.reset_coords()[onedim].isel(time=0).load()
        for l in ["latitude", "longitude"]:
            if l in ds.variables:
                del ds[l]
                ds[l] = dsone[l].copy()
                ds = ds.set_coords(l)
        coords_to_remove = [
            a for a in ds.coords if any(b in a for b in coords_patterns)
        ]
        for cr in coords_to_remove:
            del ds[cr]
        desc = cat[dsname].describe()
        desc["args"]["storage_options"]["remote_protocol"] = "file"
        dsid = "cosmo-rea-" + dsname
        if L_DASK:
            mapper_dict, ds = reset_encoding_get_mapper(
                mapper_dict, dsid, ds, desc=desc
            )
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
        dsdict[dsid] = ds
    return mapper_dict, dsdict
