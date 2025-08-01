import asyncio
from cloudify.plugins.stacer import *
from cloudify.plugins.geoanimation import *
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
from cloudify_orcestra import *
import xarray as xr
from datetime import datetime
from cloudify.utils.datasethelper import *
import os
import intake
import xpublish as xp
import fastapi
import uvicorn
#from starlette.middleware.cors import CORSMiddleware
import nest_asyncio
nest_asyncio.apply()

os.environ["FORWARDED_ALLOW_IPS"] = "127.0.0.1"

from intake.config import conf
conf["cache_disabled"] = True
L_DASK = True
# CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
# ADDRESS="tcp://127.0.0.1:42577"

#from concurrent.futures import ThreadPoolExecutor
#async def set_custom_executor():
#    executor = ThreadPoolExecutor(max_workers=100)
#    asyncio.get_event_loop().set_default_executor(executor)
#    print("ThreadPoolExecutor set to 1 worker.")

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
mapper_dict = {}    
dsdict = {}

async def start_all_datasets():
    global collection, L_DASK, kp, mapper_dict, dsdict
    await asyncio.sleep(0)    
    L_NEXTGEMS = False #True
    L_ORCESTRA = True #True #True
    L_COSMOREA = False #True #True
    L_ERA5 = True #True
    L_DYAMOND = True #True #True
    L_EERIE = False # True #True
    
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
    collection.setup_datasets(dsdict)
    # collection.register_plugin(Stac())
    #collection.register_plugin(Stats())
    # collection.register_plugin(FileServe())
    # collection.register_plugin(PlotPlugin())

    #collection.serve(host="0.0.0.0", port=9000, workers=2)

app.add_event_handler("startup", start_all_datasets)


if __name__ == "__main__":  # This avoids infinite subprocess creation
    if L_DASK:
        import dask

        dask.config.set({"array.slicing.split_large_chunks": False})
        dask.config.set({"array.chunk-size": "100 MB"})
        print("Start cluster")
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

    uvicorn.run("__main__:app", host="0.0.0.0", port=9000, workers=4)
