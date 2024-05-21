import os
import intake
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.dynamic_variables import *
from cloudify.plugins.exposer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
import xarray as xr
import xpublish as xp
import asyncio
import nest_asyncio
nest_asyncio.apply()
from intake.config import conf
conf['cache_disabled'] = True
import threading
import time

def compress_data(partds):
    import numcodecs
    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))

if __name__ == "__main__":  # This avoids infinite subprocess creation
#    client = asyncio.get_event_loop().run_until_complete(get_dask_client())
    import dask
    dask.config.set({"array.slicing.split_large_chunks": False})
    dask.config.set({"array.chunk-size": "100 MB"})
    zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    #client=Client(cluster)
    os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address
    dsdict={}
    cat=intake.open_catalog("https://gitlab.dkrz.de/data-infrastructure-services/era5-kerchunks/-/raw/main/main.yaml")
    ds=cat["pressure-level_analysis_daily"].to_dask()
    ds=ds.apply_ufunc(compress_data)
    dsdict["pressure-level_analysis_daily"]=ds
    #collection = xp.Rest([], cache_kws=dict(available_bytes=0))
    #collection.register_plugin(DynamicKerchunk())
    collection = xp.Rest(dsdict,cache_kws=dict(available_bytes=1000000000))    
    #collection.register_plugin(DynamicKerchunk())
    collection.register_plugin(DynamicAdd())
    collection.register_plugin(FileServe())
    collection.register_plugin(PlotPlugin())

    stop_thread = False

    def blocking_function():
        while not stop_thread:
            collection.serve(host="0.0.0.0", port=9000)

# Create a thread for the blocking function
    thread = threading.Thread(target=blocking_function)

# Start the thread
    thread.start()

# Block the main thread for 3 seconds
    time.sleep(3)

# Set the flag to stop the thread
    stop_thread = True

# Optionally join the thread if you need to wait for it to finish
    thread.join()

