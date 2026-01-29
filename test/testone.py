import os
import intake
from cloudify.plugins.dynamic_datasets import *
from cloudify.plugins.dynamic_variables import *
from cloudify.utils.daskhelper import *
import xarray as xr
import xpublish as xp
import asyncio
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
    ds=xr.apply_ufunc(
            compress_data,
            ds,
            dask="parallelized",
            keep_attrs="drop_conflicts"
            )
    dsdict["pressure-level_analysis_daily"]=ds

    stop_thread = False

    def blocking_function():
        while True:
            collection = xp.Rest(dsdict,cache_kws=dict(available_bytes=1000000000))
            #collection.register_plugin(DynamicKerchunk())
            collection.register_plugin(DynamicAdd())
            collection.register_plugin(FileServe())
            collection.register_plugin(PlotPlugin())
            collection.serve(host="0.0.0.0", port=9000)

async def run_blocking_function_for_duration(duration):
    loop = asyncio.get_running_loop()
    # Use a ThreadPoolExecutor to run the blocking function in a separate thread
    with ThreadPoolExecutor() as pool:
        # Schedule the blocking function to run in a separate thread
        future = loop.run_in_executor(pool, blocking_function)
        # Wait for the specified duration
        await asyncio.sleep(duration)
        # Cancel the future to stop the blocking function
        future.cancel()

    stop_thread=False
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


