from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.dynamic_variables import *
from cloudify.plugins.exposer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
import os
import numcodecs
import xarray as xr
import xpublish as xp
import intake
import tqdm
import asyncio
import nest_asyncio
nest_asyncio.apply()
from intake.config import conf
conf['cache_disabled'] = True

#CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
CATALOG_FILE="/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
#ADDRESS="tcp://127.0.0.1:42577"

def compress_data(partds):
    import numcodecs
    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))

def find_data_sources(catalog,name=None):
    newname='.'.join(
        [ a 
         for a in [name, catalog.name]
         if a
        ]
    )
    data_sources = []

    for key, entry in catalog.items():
        if isinstance(entry,intake.source.csv.CSVSource):
            continue
        if isinstance(entry, intake.catalog.Catalog):
            if newname == "main":
                newname = None
            # If the entry is a subcatalog, recursively search it
            data_sources.extend(find_data_sources(entry, newname))
        elif isinstance(entry, intake.source.base.DataSource):
            data_sources.append(newname+"."+key)

    return data_sources


def list_pickles(path):
    file_list = []

    for root, _, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)

    return file_list

def split_ds(ds):
    fv=None
    for v in ds.data_vars:
        if "time" in ds[v].dims:
            fv=v
            break
    if not fv:
        return [ds]
    dimno=0
    for dimno,dim in enumerate(ds[v].dims):
        if dim == "time":
            break
    first=ds[fv].chunks[dimno][0]
    subdss=[]
    sumchunks=0
    lastsum=0
    for i in range(len(ds[fv].chunks[0])):
        f=ds[fv].chunks[dimno][i]
        sumchunks+=f
        if f != first:
            dss=ds.isel(time=slice(lastsum,sumchunks))
            subdss.append(dss)
            lastsum=sumchunks
    if not subdss:
        return [ds]
    if len(subdss) == 1:
        return [ds]
    return subdss

if __name__ == "__main__":  # This avoids infinite subprocess creation
#    client = asyncio.get_event_loop().run_until_complete(get_dask_client())
    import dask
    dask.config.set({"array.slicing.split_large_chunks": False})
    dask.config.set({"array.chunk-size": "100 MB"})
    zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    #cluster.adapt(
    #        target_duration="0.1s",
    #        minimum=2,
    #        maximum=6,
#            minimum_cores=2,
#            maximum_cores=2,
    #        minimum_memory="16GB",
    #        maximum_memory="48GB"
    #        )
    #client=Client(cluster)
    os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address
    cat=intake.open_catalog(CATALOG_FILE)
    hostids=[]
    for source in list(cat["model-output"]):
        if source != "csv" and source != "esm-json":
            hostids+=find_data_sources(cat["model-output"][source])
    print(hostids)
    hostids+=find_data_sources(cat["observations.ERA5.era5-dkrz"])
    dsdict={}
    for dsid in hostids:
        listref=False
        if not dsid.startswith("era5-dkrz"):
            testurl=cat["model-output"][dsid].describe()["args"]["urlpath"]
            if type(testurl)==list:
                testurl=testurl[0]
                listref=True
            if "bm1344" not in testurl and "bk1377" not in testurl:
                continue
            if "3d_grid" in dsid:
                continue
            if not (testurl.startswith("reference::") or testurl.startswith('"reference::') ):
                print(testurl)
                continue
        else:
            testurl=cat["observations.ERA5"][dsid].describe()["args"]["urlpath"]
            if type(testurl)==list:
                testurl=testurl[0]
        try:
            print(dsid)
            if dsid.startswith("era5-dkrz"):
                if "hourly" in dsid and not "surface" in dsid:
                    continue
                ds=cat["observations.ERA5"][dsid].to_dask()
            else:
                ds=cat["model-output"][dsid].to_dask()
        except Exception as e:
            print("Could not load:")
            print(e)
            continue
        ppurl=testurl.replace("reference::/","").replace("kerchunks_batched","kerchunks_pp").replace("combined.parq","merged.json")
        print(ppurl)
        if os.path.isfile(ppurl):
            ds.attrs["ppurl"]="https://eerie.cloud.dkrz.de/static/"+'/'.join(ppurl.split('/')[4:])
        ds.attrs["_catalog_id"]=dsid
        if "d" in ds.data_vars:
            ds=ds.rename(d="testd")

        if "fesom" in dsid:
            chunk_dict = dict(nod2=1000000, nz1_upper=6, nz1=6, nz_upper=6, nz=6, time=4)
            keylist = [k for k in chunk_dict.keys() if k not in ds.dims]
            if "heightAboveGround" in ds.variables:
                ds = ds.drop("heightAboveGround")
            for k in keylist:
                del chunk_dict[k]
            ds = ds.chunk(chunk_dict)
        if "hadgem" in dsid:
            droplist = [a for a in ["height"] if a in ds.variables]
            if droplist:
                ds = ds.drop(droplist)
        ds = ds.reset_encoding()
        to_coords=[a for a in ds.data_vars if a in ["cell_sea_land_mask","lat","lon","coast","time_bnds","vertices_latitude","vertices_longitude"]]
        if to_coords:
            ds = ds.set_coords(to_coords)
        #if dsid.startswith("ifs-amip"):
        #    ds = ds.rename({'value':'latlon'}).set_index(latlon=("lat","lon")).unstack("latlon")
        elif "native" in dsid:
            ds=xr.apply_ufunc(
                    compress_data,
                    ds,
                    dask="parallelized",
                    keep_attrs="drop_conflicts"
                    )
        for var in ds.data_vars:
        #                ds[var].encoding["compressor"]=None
            ds[var].encoding = {
                    'compressor': numcodecs.Blosc(cname='lz4', clevel=5, shuffle=2, blocksize=0),
                    }
        if listref:
            splitted_ds=split_ds(ds)
            for idx,dss in enumerate(splitted_ds):
                dsdict[dsid+f".{idx}"]=dss
        else:
            dsdict[dsid]=ds
    #collection = xp.Rest([], cache_kws=dict(available_bytes=0))
    #collection.register_plugin(DynamicKerchunk())
    collection = xp.Rest(dsdict,cache_kws=dict(available_bytes=1000000000))    
    #collection.register_plugin(DynamicKerchunk())
    collection.register_plugin(DynamicAdd())
    collection.register_plugin(FileServe())
    collection.register_plugin(PlotPlugin())

    collection.serve(host="0.0.0.0", port=9000)
