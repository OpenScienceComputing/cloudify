from typing import Sequence, Any, Generator
from fastapi import APIRouter, Depends, HTTPException
import asyncio
#from fastapi.responses import HTMLResponse

# fro mZarr:
# from starlette.responses import StreamingResponse, Response  # type: ignore
from fastapi.responses import StreamingResponse, Response
import cachey
import xarray as xr
#import fsspec
# import asyncio
import gc
import json
import numpy as np
import math

from xpublish import Plugin, hookimpl, Dependencies
from xpublish.utils.api import DATASET_ID_ATTR_KEY
from datetime import datetime
from cloudify.utils.datasethelper import *

# Constants for garbage collection
GCLIMIT = 500
gctrigger = 0

todaystring = datetime.today().strftime("%a, %d %b %Y %H:%M:%S GMT")


async def kerchunk_stream_content_safe(
    fsmap: Any, key: str,
    **kwargs
) -> Generator[bytes, None, None]:
    try:
        yield await fsmap.asyncitem(key, **kwargs)  # This fails immediately if key is invalid (lazy access)
    except:
        # Let the outer logic raise 404 before returning a StreamingResponse
        raise
         
def kerchunk_stream_content_safe_sync(
    fsmap: Any, key: str
) -> Generator[bytes, None, None]:
    try:
        #yield await asyncio.to_thread(fsmap.__getitem__,key)  # This fails immediately if key is invalid (lazy access)
        yield fsmap[key]  # This fails immediately if key is invalid (lazy access)
    except:
        # Let the outer logic raise 404 before returning a StreamingResponse
        raise

def create_response_for_zmetadata(zm, key):
    if key.endswith(".zgroup"):
        jsondump = json.dumps({"zarr_format": 2}) #.encode("utf-8")
        return Response(jsondump, media_type="application/json")

    zmetadata = json.loads(zm.decode("utf-8"))
    zmetadata = sanitize_for_json(zmetadata)
    if key == ".zmetadata" or key == "zarr.json":
        jsondump = json.dumps(zmetadata)
        if key == ".zmetadata":
            zmetadata["zarr_consolidated_format"] = 1
    elif key.endswith("zarr.json"):
        cm = zmetadata.get("consolidated_metadata",{})
        md = cm.get("metadata",{})
        km = key.split('/')[-2]
        jsondump = md.get(km,{})
    else:
        jsondump = zmetadata["metadata"].get(key)

    if jsondump:
        jsondump = json.dumps(jsondump)
    else:
        return Respose(status_code=404)

    return Response(jsondump, media_type="application/json")
    
async def get_zarr_config_response_async(dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap):
    cache_key = dataset.attrs.get(DATASET_ID_ATTR_KEY, "") + "/kerchunk/" + f"{key}"
    resp = cache.get(cache_key)
    if resp is None:
        metakey=".zmetadata"
        if key.endswith("zarr.json"):
            metakey="zarr.json"
        try:
            zm = await fsmap.asyncitem(metakey)
        except:
            raise FileNotFoundError(metakey)
        resp = create_response_for_zmetadata(zm, key)
        cache.put(cache_key, resp, 999)
    return resp

def get_zarr_config_response(dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap):
    cache_key = dataset.attrs.get(DATASET_ID_ATTR_KEY, "") + "/kerchunk/" + f"{key}"
    resp = cache.get(cache_key)
    if resp is None:
        metakey=".zmetadata"
        if key.endswith("zarr.json"):
            metakey="zarr.json"        
        try:
            zm = fsmap[metakey]
        except:
            raise FileNotFoundError(metakey)        
        resp = create_response_for_zmetadata(zm, key)
        cache.put(cache_key, resp, 999)        
    return resp


def set_headers_and_clear_garbage(resp):
    global gctrigger
    # Common headers
    resp.headers["X-EERIE-Request-Id"] = "True"
    resp.headers["Last-Modified"] = todaystring
    resp.headers["Cache-control"] = "max-age=604800"     

    if gctrigger > GCLIMIT:
        print("Run Gccollect")
        gc.collect()
        gctrigger = 0
    gctrigger += 1
    return resp

def get_source(encoding):
    if "source" not in encoding:
        raise HTTPException(status_code=404, detail="Dataset is not kerchunk-passable")

    return encoding["source"]

async def get_full_stream(gen):
    first = await anext(gen)

    async def full_stream():
        yield first
        async for chunk in gen:
            yield chunk

    return full_stream    

def handle_exception(e, tape):
    status_code=503
    if isinstance(e, FileNotFoundError) or isinstance(e, KeyError):
        status_code = 404
    
    resp = Response(status_code=status_code)
    resp.headers["X-EERIE-Request-Id"] = "True"    
    if tape:
        resp.headers["Retry-After"] = str(60*60*3)
    return resp

class KerchunkPlugin(Plugin):
    """
    Kerchunk plugin for xpublish that provides kerchunk-based data access.

    This plugin extends xpublish with endpoints for kerchunk-based data access,
    allowing efficient chunked data access through kerchunk references.
    """

    name: str = "kerchunk"
    mapper_dict: dict = {}

    dataset_router_prefix: str = "/kerchunk"
    dataset_router_tags: Sequence[str] = ["kerchunk"]

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )
        # ------------------------------
        # ASYNC version
        # ------------------------------
        @router.api_route("/{key:path}", methods=["GET", "HEAD"])
        async def get_chunk(
            key: str,
            dataset: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            return await self._handle_request_async(
                key, dataset, cache, tape=dataset.attrs.get("from_tape")
            )
       
        # ------------------------------
        # SYNC version
        # ------------------------------
        #@router.api_route("-sync/{key:path}", methods=["GET", "HEAD"])
        #def get_chunk_sync(
        #    key: str,
        #    dataset: xr.Dataset = Depends(deps.dataset),
        #    cache: cachey.Cache = Depends(deps.cache),
        #):            
        #    return self._handle_request_sync(key,dataset,cache)
        
        @router.api_route("-tape-order/{key:path}", methods=["GET", "HEAD"])
        async def order_tape_chunk(
            key: str,
            dataset: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            
            return await self._handle_request_tape_order(
                key, dataset, cache, tape=dataset.attrs.get("from_tape")
            )
        
        return router
        
    async def _handle_request_tape_order(self, key, dataset, cache, tape=False):
        global gctrigger
        sp = get_source(dataset.encoding)
        fsmap = self.mapper_dict[sp]        
        
        resp = None
        try:
        #if True:
            if any(a in key for a in [".zmetadata", ".zarray", ".zgroup", ".zattrs", "zarr.json"]):
                resp = await get_zarr_config_response_async(
                        dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap
                        )
            else:
                gen = None
                if not any(b in key for b in ["time","lat/","lon/"]):            
                    resp = Response(status_code=404)                    
                    try:
                    #if True:
                        gen = kerchunk_stream_content_safe(fsmap, key, start=0,end=1)
                        first = await anext(gen)
                    except Exception as e:
                        print(e)
                        pass
                else:
                    gen = kerchunk_stream_content_safe(fsmap, key)
                    full_stream = await get_full_stream(gen)
                    resp = StreamingResponse(full_stream(), media_type="application/octet-stream")   
            resp = set_headers_and_clear_garbage(resp)                    
                    
        except Exception as e:
            resp = handle_exception(e, tape)
        return resp   
                    
    async def _handle_request_async(self, key, dataset, cache, tape=False):
        global gctrigger

        sp = get_source(dataset.encoding)
        fsmap = self.mapper_dict[sp]
        
        resp = None      
        
        #if dynamic:
        #    fsmap = async_get_mapper(sp, **fsmap.use_options)
        #    self.mapper_dict[sp] = fsmap        
        try:
        #if True:
            if any(a in key for a in [".zmetadata", ".zarray", ".zgroup", ".zattrs", "zarr.json"]):
                resp = await get_zarr_config_response_async(
                        dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap
                        )                
            else:
                gen = kerchunk_stream_content_safe(fsmap, key)
                full_stream = await get_full_stream(gen)
                resp = StreamingResponse(full_stream(), media_type="application/octet-stream")

            fsmap.fs.dircache.clear()
            resp = set_headers_and_clear_garbage(resp)            
        except Exception as e:
            #raise HTTPException(status_code=404, detail="Key error in reference dict")
            resp = handle_exception(e, tape)
        return resp   

    def _handle_request_sync(self, key, dataset, cache):
        global gctrigger
        if "source" not in dataset.encoding:
            raise HTTPException(status_code=404, detail="Dataset is not kerchunk-passable")
            
        sp = dataset.encoding["source"]
        fsmap = self.mapper_dict[sp]                    
        
        try:
        #if True:
            if any(a in key for a in [".zmetadata", ".zarray", ".zgroup", ".zattrs"]):
                resp = get_zarr_config_response(
                    dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap
                )                
            else:
                gen = kerchunk_stream_content_safe_sync(fsmap, key)
                first = next(gen)                

                def full_stream():
                    yield first
                    for chunk in gen:
                        yield chunk

                resp = StreamingResponse(full_stream(), media_type="application/octet-stream")
            fsmap.fs.dircache.clear()
            resp = set_headers_and_clear_garbage(resp)
            return resp
                
        
        except Exception:
            #raise HTTPException(status_code=404, detail="Key error in reference dict")
            resp = Response(status_code=404, detail="Key error in reference dict")
            resp = set_headers_and_clear_garbage(resp)
            return resp            
