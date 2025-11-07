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

from xpublish import Plugin, hookimpl, Dependencies
from xpublish.utils.api import DATASET_ID_ATTR_KEY
from datetime import datetime
from cloudify.utils.datasethelper import *

# Constants for garbage collection
GCLIMIT = 500
gctrigger = 0

todaystring = datetime.today().strftime("%a, %d %b %Y %H:%M:%S GMT")


async def kerchunk_stream_content_safe(
    fsmap: Any, key: str
) -> Generator[bytes, None, None]:
    try:
        yield await fsmap.asyncitem(key)  # This fails immediately if key is invalid (lazy access)
    except KeyError:
        # Let the outer logic raise 404 before returning a StreamingResponse
        raise
         
def kerchunk_stream_content_safe_sync(
    fsmap: Any, key: str
) -> Generator[bytes, None, None]:
    try:
        #yield await asyncio.to_thread(fsmap.__getitem__,key)  # This fails immediately if key is invalid (lazy access)
        yield fsmap[key]  # This fails immediately if key is invalid (lazy access)
    except KeyError:
        # Let the outer logic raise 404 before returning a StreamingResponse
        raise

        
def clean_json(obj: Any) -> Any:
    """
    Clean JSON object by removing None and NaN values.

    This function recursively cleans a JSON object by removing None values
    and NaN values from dictionaries and lists.

    Args:
        obj: Input JSON object (dict, list, or primitive)

    Returns:
        Any: Cleaned JSON object with None and NaN values removed
    """
    if isinstance(obj, dict):
        return {
            k: clean_json(v)
            for k, v in obj.items()
            if v is not None and v is not np.nan
        }
    elif isinstance(obj, list):
        return [clean_json(v) for v in obj]
    else:
        return obj

def create_response_for_zmetadata(zm, key):
    zmetadata = json.loads(zm.decode("utf-8"))
    zmetadata["zarr_consolidated_format"] = 1
    if key == ".zgroup":
        jsondump = json.dumps({"zarr_format": 2}).encode("utf-8")
    elif ".zarray" in key or ".zgroup" in key or ".zattrs" in key:
        if zmetadata["metadata"].get(key):
            cleaned = clean_json(zmetadata["metadata"][key])
            jsondump = json.dumps(cleaned).encode("utf-8")
        else:
            raise HTTPException(status_code=404, detail=f"{key} not found")
    else:
        jsondump = json.dumps(zmetadata).encode("utf-8")

    return Response(jsondump, media_type="application/octet-stream")
    
async def get_zarr_config_response_async(dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap):
    cache_key = dataset.attrs.get(DATASET_ID_ATTR_KEY, "") + "/kerchunk/" + f"{key}"
    resp = cache.get(cache_key)
    if resp is None:
        zm = await fsmap.asyncitem(".zmetadata")
        resp = create_response_for_zmetadata(zm, key)
        cache.put(cache_key, resp, 999)
    return resp

def get_zarr_config_response(dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap):
    cache_key = dataset.attrs.get(DATASET_ID_ATTR_KEY, "") + "/kerchunk/" + f"{key}"
    resp = cache.get(cache_key)
    if resp is None:
        zm = fsmap[".zmetadata"]
        resp = create_response_for_zmetadata(zm, key)
        cache.put(cache_key, resp, 999)        
    return resp


def set_headers_and_clear_garbage(resp):
    global gctrigger
    # Common headers
    resp.headers["Cache-control"] = "max-age=604800"
    resp.headers["X-EERIE-Request-Id"] = "True"
    resp.headers["Last-Modified"] = todaystring

    if gctrigger > GCLIMIT:
        print("Run Gccollect")
        gc.collect()
        gctrigger = 0
    gctrigger += 1
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
            
            return await self._handle_request_async(key, dataset, cache)

        @router.api_route("-dynamic/{key:path}", methods=["GET", "HEAD"])
        async def get_chunk_dynamic(
            key: str,
            dataset: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            
            return await self._handle_request_async(key, dataset, cache, dynamic=True)
        
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
        
        return router
    
    async def _handle_request_async(self, key, dataset, cache, dynamic=False):
        global gctrigger
        if "source" not in dataset.encoding:
            raise HTTPException(status_code=404, detail="Dataset is not kerchunk-passable")
            
        sp = dataset.encoding["source"]
        fsmap = self.mapper_dict[sp]                    
        if dynamic:
            fsmap = async_get_mapper(sp, **fsmap.use_options)
            self.mapper_dict[sp] = fsmap        
        try:
        #if True:
            if any(a in key for a in [".zmetadata", ".zarray", ".zgroup", ".zattrs"]):
                resp = await get_zarr_config_response_async(
                    dataset, DATASET_ID_ATTR_KEY, key, cache,fsmap
                )                
            else:
                gen = kerchunk_stream_content_safe(fsmap, key)
                first = await anext(gen)

                async def full_stream():
                    yield first
                    async for chunk in gen:
                        yield chunk

                resp = StreamingResponse(full_stream(), media_type="application/octet-stream")

            fsmap.fs.dircache.clear()
            resp = set_headers_and_clear_garbage(resp)
            return resp
        except Exception:
            #raise HTTPException(status_code=404, detail="Key error in reference dict")
            resp = Response(status_code=404)
            resp = set_headers_and_clear_garbage(resp)
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
