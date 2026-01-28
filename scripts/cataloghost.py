import asyncio
from cloudify.plugins.stacer import *
#from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.kerchunk import *
from cloudify.plugins.dynamic_variables import *
from cloudify.plugins.statistics import *
from cloudify_cosmorea import *
from cloudify_dyamond import *
from cloudify_era5 import *
from cloudify_eerie import *
from cloudify_nextgems import *
from cloudify_orcestra2 import *
from cloudify_cordexcmip6 import *
from cloudify_epoc import *
import xarray as xr
from datetime import datetime
from cloudify.utils.datasethelper import *
import os
import intake
import xpublish as xp
import fastapi
import uvicorn
from starlette.middleware.cors import CORSMiddleware
from pathlib import Path
from filelock import FileLock
import shutil

os.environ["FORWARDED_ALLOW_IPS"] = "127.0.0.1"

TREE_DIR = Path("/tmp/tree.zarr")

from intake.config import conf
conf["cache_disabled"] = True
L_DASK = True
    
# CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
# ADDRESS="tcp://127.0.0.1:42577"

#from concurrent.futures import ThreadPoolExecutor
#async def set_custom_executor():
#    executor = ThreadPoolExecutor(max_workers=200)
#    asyncio.get_event_loop().set_default_executor(executor)
#    print("ThreadPoolExecutor set to 100 worker.")

#if __name__ == "__main__":  # This avoids infinite subprocess creation
kp = KerchunkPlugin()
collection = xp.Rest(
    #dsdict,
    #cache_kws=dict(available_bytes=100000000),
    cache_kws=dict(available_bytes=0),        
    app_kws=dict(
        redirect_slashes=False,
        dependencies=[fastapi.Depends(set_custom_header)],
        # middleware=middleware
    ),
)
collection.register_plugin(kp)
collection.register_plugin(Stac())
collection.register_plugin(Stats())
app = collection.app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

async def start_all_datasets():
    global collection, L_DASK, kp, app
    await asyncio.sleep(0)
        
    mapper_dict = {}    
    dsdict = {}    
    L_NEXTGEMS = False #True
    L_ORCESTRA = False #True #True
    L_COSMOREA = False #True #True
    L_ERA5 = False #True
    L_DYAMOND = False #True #True
    L_CORDEXCMIP6 = False
    L_EERIE = False# True #True
    L_EPOC = True
    
    if L_COSMOREA:
        mapper_dict, dsdict = add_cosmorea(mapper_dict, dsdict, l_dask=L_DASK)
        print(f"After COSMO: {len(dsdict)}")
        print(f"After COSMO: {len(mapper_dict)}")        
    if L_NEXTGEMS:
        mapper_dict, dsdict = add_nextgems(mapper_dict, dsdict, l_dask=L_DASK)
        print(f"After NEXTGEMS: {len(dsdict)}")        
        print(f"After NEXTGEMS: {len(mapper_dict)}")        
    if L_ORCESTRA:
        mapper_dict, dsdict = add_orcestra(mapper_dict, dsdict, l_dask=False)
        print(f"After ORCESTRA: {len(dsdict)}")
        print(f"After ORCESTRA: {len(mapper_dict)}")        
    if L_ERA5:
        mapper_dict, dsdict = add_era5(mapper_dict, dsdict, l_dask=L_DASK)
        len_dask=len(dsdict)
        len_m=len(mapper_dict)
        print(f"After ERA: {len_dask}")
        print(f"After ERA: {len_m}")       
    if L_DYAMOND:
        mapper_dict, dsdict = add_dyamond(mapper_dict, dsdict, l_dask=False)
        print(f"After DYAMOND: {len(dsdict)}")
        print(f"After DYAMOND: {len(mapper_dict)}")        
    if L_CORDEXCMIP6:
        mapper_dict, dsdict = add_cordexcmip6(mapper_dict, dsdict, l_dask=L_DASK)
        print(f"After DYAMOND: {len(dsdict)}")
        print(f"After DYAMOND: {len(mapper_dict)}")        
    if L_EPOC:
        mapper_dict, dsdict = add_epoc(mapper_dict, dsdict, l_dask=L_DASK)
        print(f"After EPOC: {len(dsdict)}")
        print(f"After EPOC: {len(mapper_dict)}")        
    if L_EERIE:
        mapper_dict, dsdict = add_eerie(mapper_dict, dsdict, l_dask=L_DASK)
        len_dask=len(dsdict)
        len_m=len(mapper_dict)
        print(f"After L_EERIE: {len_dask}")
        print(f"After L_EERIE: {len_m}")
        if len_dask > len_m:
            print("more dask:")
            print(set(dsdict.keys())-set(mapper_dict.keys()))
        elif len_dask < len_m:
            print("more mapper:")
            print(set(mapper_dict.keys())-set(dsdict.keys()))

    kp.mapper_dict = mapper_dict
    # collection = xp.Rest([], cache_kws=dict(available_bytes=0))
    # collection.register_plugin(DynamicKerchunk())
    # collection.register_plugin(DynamicKerchunk())
    # collection.register_plugin(DynamicAdd())
    LOCK_PATH = Path("tree_build.lock")
    #dt=xr.DataTree()
    with FileLock(str(LOCK_PATH)):
        if TREE_DIR.exists():
            print("Zarr tree already exists, skipping build.")
        else:
            print("dumping tree")            
            TREE_DIR.mkdir()
            merged_dict={}
                #list(dsdict.keys())[0]+"/zarr": create_zmetadata(list(dsdict.values())[0])
                #k+"/zarr": create_zmetadata(v)
                #for k,v in dsdict.items()
                #if not v[list(v.data_vars)[0]].chunks is None
            #}
            print("updating with kerchunk")
            for k in dsdict.keys():
                if dsdict[k].encoding.get("source") in mapper_dict:
                    try:
                    #if True:
                        merged_dict.update({
                            k+"/kerchunk": json.loads(
                                mapper_dict[dsdict[k].encoding.get("source")][".zmetadata"].decode("utf-8")
                            )
                        })
                    except:
                        print(dsdict[k].encoding.get("source"))
                else:
                    print(f"No source for {k}")
        
                ##list(dsdict.keys())[0]+"/zarr": create_zmetadata(list(dsdict.values())[0])
                #k+"/kerchunk": json.loads(
                #    mapper_dict[dsdict[k].encoding.get("source")][".zmetadata"].decode("utf-8")
                #) #fsspec.get_mapper(v)[".zmetadata"]
                #for k in dsdict.keys()
                #if dsdict[k].encoding.get("source") in mapper_dict
            #})
            merged = await consolidate_zmetadatas_for_tree(merged_dict)
            print("Succesfully created tree")
            #dt[list(dsdict.keys())[0]+"/zarr"]=list(dsdict.values())[0]
            #dt.to_zarr(
            #    "tree2.zarr",
            #    zarr_format=2,
            #    compute=False,
            #    consolidated=True
            #)
            # You now have a valid combined consolidated .zmetadata
            with open("/tmp/tree.zarr/.zmetadata", "w") as f:
                json.dump(merged, f, indent=4)
            with open("/tmp/tree.zarr/.zgroup", "w") as f:
                json.dump({"zarr_format":2}, f, indent=4)
                
    print("finalised dumping")    
    collection.setup_datasets(dsdict)
    # collection.register_plugin(Stac())
    #collection.register_plugin(Stats())
    # collection.register_plugin(FileServe())
    # collection.register_plugin(PlotPlugin())

    #collection.serve(host="0.0.0.0", port=9000, workers=2)
    for k in mapper_dict.keys():
        print(k)
    #await set_custom_executor()
    if L_DASK:
        from dask.distributed import Client   
        app.state.dask_client = Client(os.environ["ZARR_ADDRESS"])
    

app.add_event_handler("startup", start_all_datasets)


if __name__ == "__main__":  # This avoids infinite subprocess creation
    nworkers=2
    if TREE_DIR.exists() and TREE_DIR.is_dir():
        shutil.rmtree(TREE_DIR)
    
    if L_DASK : 
    #app.state.dask_client = None
    #if False :
        import dask
        dask.config.set({"array.slicing.split_large_chunks": False})
        dask.config.set({"array.chunk-size": "100 MB"})
        print("Start cluster")
        from dask.distributed import LocalCluster                
        zarrcluster = get_dask_cluster()

        #zarrcluster = loop.run_until_complete(get_dask_cluster())#localCluster(
                #processes=True,
                #n_workers=4,
                #threads_per_worker=16,
                #memory_limit="16GB",
            #)
        #)
        os.environ["ZARR_ADDRESS"] = zarrcluster.scheduler._address        

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
        
    uvicorn.run("__main__:app", host="0.0.0.0", port=9000, workers=nworkers)
