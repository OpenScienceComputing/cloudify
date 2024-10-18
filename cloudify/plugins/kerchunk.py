from typing import Sequence
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

# fro mZarr:
# from starlette.responses import StreamingResponse, Response  # type: ignore
from fastapi.responses import StreamingResponse, Response
import cachey
import xarray as xr
import fsspec
import asyncio
import gc
import json
import os

from xpublish.dependencies import (
    get_dataset,
)  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies
from xpublish.utils.api import DATASET_ID_ATTR_KEY

from datetime import datetime

todaystring = datetime.today().strftime("%a, %d %b %Y %H:%M:%S GMT")

gctrigger = 0
GCLIMIT = 500


async def kerchunk_stream_content(data):
    await asyncio.sleep(0)
    yield data


class KerchunkPass(Plugin):
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
            if True:
                if any(a in key for a in [".zmetadata", ".zarray", ".zgroup"]):
                    cache_key = (
                        dataset.attrs.get(DATASET_ID_ATTR_KEY, "")
                        + "/kerchunk/"
                        + f"{key}"
                    )
                    resp = cache.get(cache_key)
                    if resp is None:
                        zmetadata = json.loads(fsmap[".zmetadata"].decode("utf-8"))
                        if ".zmetadata" == key:
                            zmetadata["zarr_consolidated_format"] = 1
                        if key == ".zgroup":
                            jsondump = json.dumps({"zarr_format": 2}).encode("utf-8")
                        elif ".zarray" in key or ".zgroup" in key or ".zattrs" in key:
                            jsondump = json.dumps(zmetadata["metadata"][key]).encode(
                                "utf-8"
                            )
                        else:
                            jsondump = json.dumps(zmetadata).encode("utf-8")
                        resp = Response(
                            jsondump,
                            media_type="application/octet-stream",
                        )
                        cache.put(cache_key, resp, 99999)
                #                return StreamingResponse(
                #                    kerchunk_stream_content(fsmap[key]),
                #                    media_type='application/octet-stream',
                #                )
                else:
                    resp = StreamingResponse(
                        kerchunk_stream_content(fsmap[key]),
                        media_type="application/octet-stream",
                    )
                resp.headers["Cache-control"] = "max-age=604800"
                resp.headers["X-EERIE-Request-Id"] = "True"
                resp.headers["Last-Modified"] = todaystring
                resp.headers["Access-Control-Allow-Origin"] = "https://swift.dkrz.de"
                fsmap.fs.dircache.clear()
                del sp, dataset, key
                if gctrigger > GCLIMIT:
                    print("Run Gccollect")
                    gc.collect()
                    gctrigger = 0
                gctrigger += 1
                return resp
            # else:
            #    raise HTTPException(status_code=404, detail=f"Could not find key in lazy reference dict")

        return router
