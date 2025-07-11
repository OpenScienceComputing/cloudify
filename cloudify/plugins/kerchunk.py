from typing import Sequence, Any, Generator
from fastapi import APIRouter, Depends, HTTPException
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

# Constants for garbage collection
GCLIMIT = 500
gctrigger = 0

todaystring = datetime.today().strftime("%a, %d %b %Y %H:%M:%S GMT")


async def kerchunk_stream_content_safe(
    fsmap: Any, key: str
) -> Generator[bytes, None, None]:
    """
    Safely stream content from a fsspec mapper.

    This function yields content from the fsspec mapper while handling
    invalid keys gracefully.

    Args:
        fsmap: fsspec mapper object
        key: Key to access in the mapper

    Yields:
        Generator[bytes]: Content from the mapper

    Raises:
        KeyError: If the key is invalid
    """
    try:
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

        @router.api_route("/{key:path}", methods=["GET", "HEAD"])
        async def get_chunk(
            key: str,
            dataset: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
            #                use_cache=False
        ):
            global gctrigger, mapper_dict
            if "source" in dataset.encoding:
                sp = dataset.encoding["source"]
                fsmap = self.mapper_dict[sp]
            #                fsmap = fsspec.get_mapper(
            #                dataset.encoding["source"],remote_protocol="file", lazy=True,cache_size=0
            #                )
            else:
                raise HTTPException(
                    status_code=404, detail=f"Dataset ist not kerchunk-passable"
                )
            # if key in fsmap:
            try:
                #            if True:
                if any(
                    a in key for a in [".zmetadata", ".zarray", ".zgroup", ".zattrs"]
                ):
                    cache_key = (
                        dataset.attrs.get(DATASET_ID_ATTR_KEY, "")
                        + "/kerchunk/"
                        + f"{key}"
                    )
                    resp = cache.get(cache_key)
                    if resp is None:
                        zmetadata = json.loads(fsmap[".zmetadata"].decode("utf-8"))
                        zmetadata["zarr_consolidated_format"] = 1
                        if key == ".zgroup":
                            jsondump = json.dumps({"zarr_format": 2}).encode("utf-8")
                        elif ".zarray" in key or ".zgroup" in key or ".zattrs" in key:
                            if zmetadata["metadata"].get(key):
                                cleaned = clean_json(zmetadata["metadata"][key])
                                jsondump = json.dumps(cleaned).encode("utf-8")
                            else:
                                raise HTTPException(
                                    status_code=404, detail=f"{key} not  found"
                                )
                        else:
                            jsondump = json.dumps(zmetadata).encode("utf-8")
                        resp = Response(
                            jsondump,
                            media_type="application/octet-stream",
                        )
                        cache.put(cache_key, resp, 999)
                #                return StreamingResponse(
                #                    kerchunk_stream_content(fsmap[key]),
                #                    media_type='application/octet-stream',
                #                )
                else:
                    # data = await asyncio.to_thread(lambda: fsmap[key])
                    # resp = Response(
                    gen = kerchunk_stream_content_safe(fsmap, key)

                    # Try to advance once and buffer the first chunk
                    first = await anext(gen)

                    # Yield the first chunk and the rest (in this case, there likely is no 'rest')
                    async def full_stream():
                        yield first
                        async for chunk in gen:
                            yield chunk

                    resp = StreamingResponse(
                        full_stream(), media_type="application/octet-stream"
                    )
                resp.headers["Cache-control"] = "max-age=604800"
                resp.headers["X-EERIE-Request-Id"] = "True"
                resp.headers["Last-Modified"] = todaystring
                #                resp.headers['Access-Control-Allow-Origin'] = 'https://swift.dkrz.de'
                fsmap.fs.dircache.clear()
                del sp, dataset, key
                if gctrigger > GCLIMIT:
                    print("Run Gccollect")
                    gc.collect()
                    gctrigger = 0
                gctrigger += 1
                return resp
            except:
                #            else:
                raise HTTPException(
                    status_code=404, detail=f"Key error in reference dict"
                )

        return router
