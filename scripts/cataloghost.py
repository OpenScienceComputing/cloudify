from cloudify.plugins.stacer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.kerchunk import *
from cloudify.plugins.dynamic_variables import *
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
import xpublish as xp
import intake
import asyncio
import fastapi
import nest_asyncio

os.environ["FORWARDED_ALLOW_IPS"] = "127.0.0.1"
nest_asyncio.apply()
from intake.config import conf

conf["cache_disabled"] = True
L_DASK = True
L_NEXTGEMS = False #True
L_ORCESTRA = False #True
L_COSMOREA = False
L_ERA5 = True #True
L_DYAMOND = False #True
L_EERIE = False #True
mapper_dict = {}

# CATALOG_FILE="/work/bm1344/DKRZ/intake/dkrz_eerie_esm.yaml"
# ADDRESS="tcp://127.0.0.1:42577"


if __name__ == "__main__":  # This avoids infinite subprocess creation
    # client = asyncio.get_event_loop().run_until_complete(get_dask_client())
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
    dsdict = {}
    if L_COSMOREA:
        mapper_dict, dsdict = add_cosmorea(mapper_dict, dsdict)
        print(f"After COSMO: {len(dsdict)}")
        print(f"After COSMO: {len(mapper_dict)}")        
    if L_NEXTGEMS:
        mapper_dict, dsdict = add_nextgems(mapper_dict, dsdict)
        print(f"After NEXTGEMS: {len(dsdict)}")        
        print(f"After NEXTGEMS: {len(mapper_dict)}")        
    if L_ORCESTRA:
        mapper_dict, dsdict = add_orcestra(mapper_dict, dsdict)
        print(f"After ORCESTRA: {len(dsdict)}")
        print(f"After ORCESTRA: {len(mapper_dict)}")        
    if L_ERA5:
        mapper_dict, dsdict = add_era5(mapper_dict, dsdict)
        print(f"After ERA: {len(dsdict)}")
        print(f"After ERA: {len(mapper_dict)}")        
    if L_DYAMOND:
        mapper_dict, dsdict = add_dyamond(mapper_dict, dsdict)
        print(f"After DYAMOND: {len(dsdict)}")
        print(f"After DYAMOND: {len(mapper_dict)}")        
    if L_EERIE:
        mapper_dict, dsdict = add_eerie(mapper_dict, dsdict)
        print(f"After EERIE: {len(dsdict)}")
        print(f"After EERIE: {len(mapper_dict)}")        

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

    collection.serve(host="0.0.0.0", port=9000)
