from copy import deepcopy as copy
from cloudify.plugins.stacer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.kerchunk import *
from cloudify.plugins.dynamic_variables import *
from datetime import datetime
from datasethelper import *
import os
import numcodecs
import xarray as xr
import xpublish as xp
import intake
import yaml
import tqdm
import asyncio
import fastapi
import itertools
import nest_asyncio

os.environ["FORWARDED_ALLOW_IPS"] = "127.0.0.1"
nest_asyncio.apply()
from intake.config import conf

conf["cache_disabled"] = True
L_DASK = True
L_NEXTGEMS = False
L_ERA5 = False
WHITELIST_MODEL="icon-esm-er.eerie-control-1950.v20240618"
WHITELIST_MODEL="ifs-fesom2-sr.hist-1950.v20240304"
mapper_dict = {}

# CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
CATALOG_FILE = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
# ADDRESS="tcp://127.0.0.1:42577"
def set_custom_header(response: fastapi.Response) -> None:
    response.headers["Cache-control"] = "max-age=3600"
    response.headers["X-EERIE-Request-Id"] = "True"
    response.headers["Last-Modified"] = datetime.today().strftime(
        "%a, %d %b %Y 00:00:00 GMT"
    )


def compress_data(partds):
    import numcodecs

    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))


def split_ds(ds):
    fv = None
    for v in ds.data_vars:
        if "time" in ds[v].dims:
            fv = v
            break
    if not fv:
        return [ds]
    dimno = 0
    for dimno, dim in enumerate(ds[v].dims):
        if dim == "time":
            break
    first = ds[fv].chunks[dimno][0]
    subdss = []
    sumchunks = 0
    lastsum = 0
    for i in range(len(ds[fv].chunks[0])):
        f = ds[fv].chunks[dimno][i]
        sumchunks += f
        if f != first:
            dss = ds.isel(time=slice(lastsum, sumchunks))
            subdss.append(dss)
            lastsum = sumchunks
    if not subdss:
        return [ds]
    if len(subdss) == 1:
        return [ds]
    return subdss


def get_options(desc):
    options = dict(storage_options=desc["args"].get("storage_options", {}))
    if L_DASK:
        options["storage_options"].update(STORAGE_OPTIONS)
    else:
        options["chunks"] = None
    return options


if __name__ == "__main__":  # This avoids infinite subprocess creation
    # client = asyncio.get_event_loop().run_until_complete(get_dask_client())
    if L_DASK:
        import dask

        dask.config.set({"array.slicing.split_large_chunks": False})
        dask.config.set({"array.chunk-size": "100 MB"})
        zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
        # cluster.adapt(
        #        target_duration="0.1s",
        #        minimum=2,

        #        maximum=6,
        #            minimum_cores=2,
        #            maximum_cores=2,
        #        minimum_memory="16GB",
        #        maximum_memory="48GB"
        #        )
        # client=Client(cluster)
        os.environ["ZARR_ADDRESS"] = zarrcluster.scheduler._address
    cat = intake.open_catalog(CATALOG_FILE)
    hostids = []
    for source in list(cat["model-output"]):
        if source != "csv" and source != "esm-json":
            hostids += find_data_sources(cat["model-output"][source])
    print(hostids)
    hostids += find_data_sources(cat["observations.ERA5.era5-dkrz"])
    dsdict = {}
    if L_NEXTGEMS:
        mapper_dict, dsdict = add_nextgems(mapper_dict, dsdict)
    for dsid in hostids:
        if not WHITELIST_MODEL in dsid:
            continue
        desc = None
        listref = False
        if not dsid.startswith("era5-dkrz"):
            desc = cat["model-output"][dsid].describe()
            testurl = desc["args"]["urlpath"]
            if type(testurl) == list:
                testurl = testurl[0]
                listref = True
            if "bm1344" not in testurl and "bk1377" not in testurl:
                continue
            if "3d_grid" in dsid:
                continue
            if not (
                testurl.startswith("reference::") or testurl.startswith('"reference::')
            ):
                print(testurl)
                continue
        else:
            if L_ERA5:
                desc = cat["observations.ERA5"][dsid].describe()
                testurl = desc["args"]["urlpath"]

                if type(testurl) == list:
                    testurl = testurl[0]
            else:
                continue
        try:
            # if True:
            print(dsid)
            options = get_options(desc)
            if dsid.startswith("era5-dkrz"):
                if not L_ERA5:
                    continue
                if "hourly" in dsid and not "surface" in dsid:
                    continue
                ds = cat["observations.ERA5"][dsid](**options).to_dask()
            else:
                if "icon" in dsid and "native" in dsid and "2d_monthly_mean" in dsid:
                    options["chunks"] = {}
                ds = cat["model-output"][dsid](**options).to_dask()
        except Exception as e:
            # else:
            print("Could not load:")
            print(e)
            continue
        ds.attrs["_catalog_id"] = dsid
        if "d" in ds.data_vars:
            ds = ds.rename(d="testd")

        if "fesom" in dsid :
            if not L_DASK:
                continue
            chunk_dict = dict(
                nod2=1000000, nz1_upper=6, nz1=6, nz_upper=6, nz=6, time=4
            )
            keylist = [k for k in chunk_dict.keys() if k not in ds.dims]
            if "heightAboveGround" in ds.variables:
                ds = ds.drop("heightAboveGround")
            for k in keylist:
                del chunk_dict[k]
            if L_DASK:
                ds = ds.chunk(chunk_dict)
        if "hadgem" in dsid:
            droplist = [a for a in ["height"] if a in ds.variables]
            if droplist:
                ds = ds.drop(droplist)
        to_coords = [
            a
            for a in ds.data_vars
            if a
            in [
                "cell_sea_land_mask",
                "lat",
                "lon",
                "coast",
                "time_bnds",
                "vertices_latitude",
                "vertices_longitude",
            ]
        ]
        if to_coords:
            ds = ds.set_coords(to_coords)
        if dsid.startswith("ifs-") and "gr025" in dsid and "value" in ds.dims:
            ds = ds.reset_encoding()
            ds = ds.rename({'value':'latlon'}).set_index(latlon=("lat","lon")).unstack("latlon")
        if "native" in dsid and not "grid" in dsid:
            print("lossy")
            if L_DASK:
                ds = xr.apply_ufunc(
                    compress_data, ds, dask="parallelized", keep_attrs="drop_conflicts"
                )
            else:
                mapper_dict, ds = reset_encoding_get_mapper(
                    mapper_dict, dsid, ds, desc=desc
                )
                for var in ds.data_vars:
                    ds[var].encoding["filters"] = numcodecs.BitRound(keepbits=12)
        if not "grid" in dsid:
            ds = set_compression(ds)

        if listref:
            splitted_ds = split_ds(ds)
            for idx, dss in enumerate(splitted_ds):
                newid = dsid + f".{idx}"
                dss = adapt_for_zarr_plugin_and_stac(dsid, dss)
                dsdict[newid] = dss
        else:
#            if L_DASK:
#                mapper_dict, ds = reset_encoding_get_mapper(
#                    mapper_dict, dsid, ds, desc=desc
#                )
            ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
            dsdict[dsid] = ds
    dsdict["ifs-fesom2-sr.hist-1950.v20240304.atmos.gr025.2D_monthly_avg"]=dsdict["ifs-fesom2-sr.hist-1950.v20240304.atmos.gr025.2D_daily_avg"].resample(time="1M").mean()
    kp = KerchunkPass()
    kp.mapper_dict = mapper_dict
    # collection = xp.Rest([], cache_kws=dict(available_bytes=0))
    # collection.register_plugin(DynamicKerchunk())
    collection = xp.Rest(
        dsdict,
        cache_kws=dict(available_bytes=100000000),
        app_kws=dict(
            redirect_slashes=False,
            dependencies=[fastapi.Depends(set_custom_header)]
            # middleware=middleware
        ),
    )
    # collection.register_plugin(DynamicKerchunk())
    collection.register_plugin(DynamicAdd())
    collection.register_plugin(kp)
    collection.register_plugin(Stac())
    # collection.register_plugin(FileServe())
    # collection.register_plugin(PlotPlugin())

    collection.serve(host="0.0.0.0", port=80)
