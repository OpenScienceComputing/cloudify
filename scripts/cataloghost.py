from cloudify.plugins.exposer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.kerchunk import *
from cloudify.plugins.dynamic_variables import *
from cloudify.plugins.stacer import *
from copy import deepcopy as copy
from stacer import *
from datetime import datetime
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
STORAGE_OPTIONS = dict(
    #        remote_protocol="file",
    lazy=True,
    cache_size=0,
    #        skip_instance_cache=True,
    #        listings_expiry_time=0
)
L_DASK = True
L_NEXTGEMS = True
mapper_dict = {}

# CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
CATALOG_FILE = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
# ADDRESS="tcp://127.0.0.1:42577"


def set_custom_header(response: fastapi.Response) -> None:
    response.headers["Cache-control"] = "max-age=604800"
    response.headers["X-EERIE-Request-Id"] = "True"
    response.headers["Last-Modified"] = datetime.today().strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )


def refine_nextgems(iakey, ds, md, desc):
    ds = reset_encoding_get_mapper(iakey, ds, desc=desc)
    ds = adapt_for_zarr_plugin_and_stac(iakey, ds)
    ds = set_compression(ds)
    for mdk, mdv in md.items():
        if not mdk in ds.attrs:
            ds.attrs[mdk] = mdv
    for mdk, mdv in desc["metadata"].items():
        if not mdk in ds.attrs:
            ds.attrs[mdk] = mdv
    return ds


def add_nextgems(dsdict):
    NGC_PROD_CAT = "https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml"
    #    NGC_PROD_CAT="https://data.nextgems-h2020.eu/catalog.yaml"
    ngccat = intake.open_catalog(NGC_PROD_CAT)
    md = yaml.safe_load(ngccat.yaml())["sources"]["nextGEMS_prod"]["metadata"]
    dsadd = [
        "ICON.ngc4008",
        "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512",
        "IFS.IFS_9-FESOM_5-production.3D_hourly_healpix512",
    ]
    for ia in dsadd:
        desc = ngccat[ia].describe()
        if type(desc["args"]["urlpath"]) == str and desc["args"]["urlpath"].endswith(
            "zarr"
        ):
            if "user_parameters" in desc:
                ups = desc["user_parameters"]
                uplists = [up["allowed"] for up in ups]
                combinations = list(itertools.product(*uplists))
                combdict = [
                    {ups[i]["name"]: comb[i] for i in range(len(ups))}
                    for comb in combinations
                ]
                for comb in combdict:
                    iakey = ia + "." + "_".join([str(a) for a in list(comb.values())])
                    try:
                        dsdict[iakey] = ngccat[ia](**comb, chunks="auto").to_dask()
                        dsdict[iakey] = refine_nextgems(iakey, dsdict[iakey], md, desc)
                    except Exception as e:
                        print(e)
                        pass
        else:
            iakey = ".".join(ia.split(".")[1:])
            dsdict[iakey] = ngccat[ia](chunks="auto").to_dask()
            dsdict[iakey] = refine_nextgems(iakey, dsdict[iakey], md, desc)
    print(dsdict.keys())
    return dsdict


def compress_data(partds):
    import numcodecs

    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))


def find_data_sources(catalog, name=None):
    newname = ".".join([a for a in [name, catalog.name] if a])
    data_sources = []

    for key, entry in catalog.items():
        if isinstance(entry, intake.source.csv.CSVSource):
            continue
        if isinstance(entry, intake.catalog.Catalog):
            if newname == "main":
                newname = None
            # If the entry is a subcatalog, recursively search it
            data_sources.extend(find_data_sources(entry, newname))
        elif isinstance(entry, intake.source.base.DataSource):
            data_sources.append(newname + "." + key)

    return data_sources


def list_pickles(path):
    file_list = []

    for root, _, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)

    return file_list


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


def reset_encoding_get_mapper(dsid, ds, desc=None):
    global mapper_dict
    sp = None
    if "source" in ds.encoding:
        sp = ds.encoding["source"]
    elif desc:
        updesc = desc["args"]["urlpath"]
        if type(updesc) == str or (type(updesc) == list and len(updesc) == 1):
            if type(updesc) == list:
                updesc = updesc[0]
            sp = updesc
    ds = ds.reset_encoding()
    if sp:
        use_options = copy(STORAGE_OPTIONS)
        if desc:
            use_options.update(desc["args"].get("storage_options", {}))
        mapper_dict[sp] = fsspec.get_mapper(sp, **use_options)
        ds.encoding["source"] = sp
    return ds


def get_options(desc):
    options = dict(storage_options=desc["args"].get("storage_options", {}))
    if L_DASK:
        options["storage_options"].update(STORAGE_OPTIONS)
    else:
        options["chunks"] = None
    return options


def set_compression(ds):
    for var in ds.data_vars:
        #                ds[var].encoding["compressor"]=None
        ds[var].encoding = {
            "compressor": numcodecs.Blosc(
                cname="lz4", clevel=5, shuffle=1, blocksize=0
            ),
        }
    return ds


def adapt_for_zarr_plugin_and_stac(dsid, ds):
    stac_attrs = ["title", "description"]
    for att in stac_attrs:
        if att not in ds.attrs:
            ds.attrs[att] = dsid
    if "time" in ds.variables:
        ds["time"].encoding["dtype"] = "float64"
        ds["time"].encoding["compressor"] = None
        ds.attrs["time_min"] = str(ds["time"].values[0])
        ds.attrs["time_max"] = str(ds["time"].values[-1])

    ds.attrs["creation_date"] = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    return ds


if __name__ == "__main__":  # This avoids infinite subprocess creation
    client = asyncio.get_event_loop().run_until_complete(get_dask_client())
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
        dsdict = add_nextgems(dsdict)
    for dsid in hostids:
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
            desc = cat["observations.ERA5"][dsid].describe()
            testurl = desc["args"]["urlpath"]

            if type(testurl) == list:
                testurl = testurl[0]
        try:
            # if True:
            print(dsid)
            options = get_options(desc)
            if dsid.startswith("era5-dkrz"):
                if "hourly" in dsid and not "surface" in dsid:
                    continue
                ds = cat["observations.ERA5"][dsid](**options).to_dask()
            else:
                ds = cat["model-output"][dsid](**options).to_dask()
        except Exception as e:
            # else:
            print("Could not load:")
            print(e)
            continue
        ds.attrs["_catalog_id"] = dsid
        if "d" in ds.data_vars:
            ds = ds.rename(d="testd")

        if "fesom" in dsid:
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
        if L_DASK:
            ds = reset_encoding_get_mapper(dsid, ds, desc=desc)
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
        # if dsid.startswith("ifs-amip"):
        #    ds = ds.rename({'value':'latlon'}).set_index(latlon=("lat","lon")).unstack("latlon")
        if "native" in dsid and not "grid" in dsid:
            print("lossy")
            if L_DASK:
                ds = xr.apply_ufunc(
                    compress_data, ds, dask="parallelized", keep_attrs="drop_conflicts"
                )
            else:
                for var in ds.data_vars:
                    ds[var].encoding["filters"] = numcodecs.BitRound(keepbits=12)
        if not "grid" in dsid:
            ds = set_compression(ds)
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)

        if listref:
            splitted_ds = split_ds(ds)
            for idx, dss in enumerate(splitted_ds):
                dsdict[dsid + f".{idx}"] = dss
        else:
            dsdict[dsid] = ds
    kp = KerchunkPass()
    kp.mapper_dict = mapper_dict
    # collection = xp.Rest([], cache_kws=dict(available_bytes=0))
    # collection.register_plugin(DynamicKerchunk())
    collection = xp.Rest(
        dsdict,
        cache_kws=dict(available_bytes=100000000),
        app_kws=dict(
            redirect_slashes=False,
            dependencies=[fastapi.Depends(set_custom_header)],
            # middleware=middleware
        ),
    )
    # collection.register_plugin(DynamicKerchunk())
    collection.register_plugin(DynamicAdd())
    collection.register_plugin(kp)
    collection.register_plugin(Stac())
    # collection.register_plugin(FileServe())
    collection.register_plugin(PlotPlugin())

    collection.serve(host="0.0.0.0", port=9000)
