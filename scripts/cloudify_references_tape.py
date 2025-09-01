
#ssl_keyfile="/work/bm0021/k204210/cloudify/workshop/key.pem"
#ssl_certfile="/work/bm0021/k204210/cloudify/workshop/cert.pem"

from cloudify.utils.daskhelper import *
from cloudify.plugins.kerchunk import *
import xarray as xr
import xpublish as xp
import asyncio
import sys
import os
import socket   
import fsspec
from contextlib import closing

SO=dict(
    remote_protocol="slk",
    remote_options=dict(
        slk_cache="/scratch/k/k202134/INTAKE_CACHE"
    ),
    lazy=True,
    cache_size=0
)

def find_free_port_in_range(start=9000, end=9100):
    for port in range(start, end + 1):
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.bind(('', port))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return port
        except:
            continue
    raise RuntimeError("No free port found in the specified range.")


if __name__ == "__main__":  # This avoids infinite subprocess creation
    #import dask
    #zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    #os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address
    
    dsname=sys.argv[1]
    glob_inp=sys.argv[2]

    dsdict={}
    mapper_dict={}
    
    source="reference::/"+glob_inp    
    fsmap = fsspec.get_mapper(
            source,
            **SO
            )
    ds=xr.open_dataset(
            fsmap,
            engine="zarr",
            chunks="auto",
            consolidated=False
            )
    mapper_dict[source]=fsmap
    ds=ds.drop_encoding()
    ds.encoding["source"]=source
    dsdict[dsname]=ds

    kp = KerchunkPlugin()
    kp.mapper_dict = mapper_dict    
    
    collection = xp.Rest(dsdict)
    collection.register_plugin(kp)
    freeport=find_free_port_in_range()
    listen_uri_fn=f"{os.environ['HOSTNAME']}_{freeport}"
    with open(listen_uri_fn, "w"):    
        collection.serve(
            host="0.0.0.0",
            port=freeport,
            #ssl_keyfile=ssl_keyfile,
            #ssl_certfile=ssl_certfile
        )        
