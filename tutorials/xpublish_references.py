
ssl_keyfile="/work/bm0021/k204210/cloudify/workshop/key.pem"
ssl_certfile="/work/bm0021/k204210/cloudify/workshop/cert.pem"

from cloudify.plugins.stacer import *
from cloudify.utils.daskhelper import *
from cloudify.plugins.kerchunk import *
import xarray as xr
import xpublish as xp
import asyncio
#import nest_asyncio
import sys
import os
import socket

def is_port_free(port, host="localhost"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) != 0  # Returns True if the port is free

def find_free_port(start=5000, end=5100, host="localhost"):
    for port in range(start, end + 1):
        if is_port_free(port, host):
            return port
    return None  # No free ports found

port = find_free_port(9000,9100)
if not port:
    raise ValueError("Could not find a free port for service")

SO=dict(
    remote_protocol="slk",
    remote_options=dict(
        slk_cache="/scratch/k/k202134/INTAKE_CACHE"
    ),
    lazy=True,
    cache_size=0
)
#nest_asyncio.apply()


if __name__ == "__main__":  # This avoids infinite subprocess creation
    #import dask
    #zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    #os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address
    
    glob_inp=sys.argv[1:]

    dsdict={}
    mapper_dict={}
    for g in glob_inp:
        dsname=g.split('/')[-1]
        source="reference::/"+g
        print(source)
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

    kp = KerchunkPass()
    kp.mapper_dict = mapper_dict
    
    collection = xp.Rest(dsdict)
    collection.register_plugin(Stac())
    collection.register_plugin(kp)
    collection.serve(
        host="0.0.0.0",
        port=port,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile
    )
