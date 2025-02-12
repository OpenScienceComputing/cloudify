from cloudify.plugins.stacer import *
from cloudify.plugins.geoanimation import *
from cloudify.utils.daskhelper import *
import xarray as xr
import xpublish as xp
import asyncio
import nest_asyncio
import sys
import os
import subprocess
#import hdf5plugin
import argparse

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
          "This xpublish script serves a virtual zarr dataset.\n"
          "When a chunk is accessed, it processes data from any TCO gaussian reduced grid and regrids it to the matching regular gaussian grid.\n"
          "It opens files or references and uses dask to regrid chunks by linear interpolation across longitudes."
          )

    )
    
    parser.add_argument("dataset_name", help="The name of the dataset displayed for users.")
    parser.add_argument("mode", choices=["refs", "norefs"], help="Specify 'refs' or 'norefs'.")
    parser.add_argument("paths", nargs='+', help="Paths to dataset files (single for 'refs', multiple for 'norefs').")
    
    args = parser.parse_args()
    
    # Validation: If 'refs' mode, ensure exactly one path is provided
    if args.mode == "refs" and len(args.paths) != 1:
        parser.error("Mode 'refs' requires exactly one path argument.")
    
    return args


os.environ["HDF5_PLUGIN_PATH"]="/work/ik1017/hdf5plugin/plugins/"
cwd = os.getcwd()
ssl_keyfile=f"{cwd}/key.pem"
ssl_certfile=f"{cwd}/cert.pem"

if not os.path.isfile(ssl_keyfile) or not os.path.isfile(ssl_certfile):
    cn = os.uname().nodename  # Equivalent to `!echo $HOSTNAME`

    openssl_cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:4096",
            "-keyout", "key.pem", "-out", "cert.pem",
            "-sha256", "-days", "3650", "-nodes",
            "-subj", f"/C=XX/ST=Hamburg/L=Hamburg/O=Test/OU=Test/CN={cn}"
            ]

    subprocess.run(openssl_cmd, check=True)

port=9010

nest_asyncio.apply()
chunks={}
for coord in ["lon","lat"]:
    chunk_size=os.environ.get(f"XPUBLISH_{coord.upper()}_CHUNK_SIZE",None)
    if chunk_size:
        chunks[coord]=int(chunk_size)

chunks["time"]=1
l_lossy=os.environ.get("L_LOSSY",False)

def lossy_compress(partds):
    import numcodecs
    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))

def unstack(ds):
    onlydimname=[a for a in ds.dims if a not in ["time","level","lev"]]
    if not "lat" in ds.coords:
        if not "latitude" in ds.coords:
            raise ValueError("No latitude given")
        else:
            ds=ds.rename(latitude="lat")
    if not "lon" in ds.coords:
        if not "longitude" in ds.coords:
            raise ValueError("No longitude given")
        else:
            ds=ds.rename(longitude="lon")            
    if len(onlydimname)>1:
        raise ValueError("More than one dim: "+onlydimname)
    onlydimname=onlydimname[0]
    return ds.rename({onlydimname:'latlon'}).set_index(latlon=("lat","lon")).unstack("latlon")

def interp(ds):
    global equator_lons
    return ds.interpolate_na(dim="lon",method="linear",period=360.0).reindex(lon=equator_lons)

if __name__ == "__main__":  # This avoids infinite subprocess creation
    import dask
    zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    os.environ["ZARR_ADDRESS"]=zarrcluster.scheduler._address

    args = parse_args()
    print(f"Dataset Name: {args.dataset_name}")
    print(f"Mode: {args.mode}")
    print(f"Paths: {args.paths}")

    dsname=args.dataset_name
    refs=args.mode
    glob_inp=args.paths

    dsdict={}

    if refs == "refs":
        glob_inp=glob_inp[0]
        source="reference::/"+glob_inp
        fsmap = fsspec.get_mapper(
                source,
                remote_protocol="file",
                lazy=True,
                cache_size=0
                )
        ds=xr.open_dataset(
                fsmap,
                engine="zarr",
                chunks=chunks,
                consolidated=False        
                )
    else:
        ds=xr.open_mfdataset(
            glob_inp,
            compat="override",
            coords="minimal",
            chunks=chunks,
        )
    todel=[
            a
            for a in ds.coords
            if a not in ["lat","latitude","lon","longitude","time","lev","level"]
            ]
            
    if todel:
        for v in todel:
            del ds[v]
    if "height" in ds:
        del ds["height"]
    for dv in ds.variables:
        if "time" in dv:
            ds[dv]=ds[dv].load()
            ds[dv].encoding["dtype"] = "float64"
            ds[dv].encoding["compressor"] = None
    ds=ds.set_coords([a for a in ds.data_vars if "bnds" in a])
    if l_lossy:
        ds = xr.apply_ufunc(
            lossy_compress,
            ds,
            dask="parallelized", 
            keep_attrs="drop_conflicts"
        )
    dvs=[]
    l_el=False
    for dv in ds.data_vars:
        print(dv)
        template_unstack=unstack(ds[dv].isel(time=0).load())
        if not l_el:
            equator_lons=template_unstack.sel(lat=0.0,method="nearest").dropna(dim="lon")["lon"]
            l_el=True
            latlonchunks={
                    a:len(template_unstack[a])
                    for a in template_unstack.dims
                    }
        template_unstack=template_unstack.chunk(**latlonchunks)
        template_interp=interp(template_unstack)
        template_unstack=template_unstack.expand_dims(**{"time":ds["time"]}).chunk(time=1)
        template_interp=template_interp.expand_dims(**{"time":ds["time"]}).chunk(time=1)
        unstacked=ds[dv].map_blocks(unstack,template=template_unstack)
        interped=unstacked.map_blocks(interp,template=template_interp)
        dsv = dask.optimize(interped)[0]
        print("optimized")
        del template_unstack, template_interp
        dvs.append(dsv)
    ds = xr.combine_by_coords(dvs)
    print("combined")
    ds = ds.drop_encoding()
    dsdict[dsname]=ds
    collection = xp.Rest(dsdict)
    collection.register_plugin(Stac())
    collection.register_plugin(PlotPlugin())
    collection.serve(
        host="0.0.0.0",
        port=port,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile
    )
