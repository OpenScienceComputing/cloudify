from typing import Sequence
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

# fro mZarr:
# from starlette.responses import StreamingResponse, Response  # type: ignore
from fastapi.responses import StreamingResponse, Response
import xarray as xr
import fsspec
import gc
import json
import os

from xpublish.dependencies import (
    get_dataset,
)  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies

gctrigger = 0
GCLIMIT = 500


def kerchunk_stream_content(data):
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

        @router.get("/{key:path}")
        async def get_chunk(
            key: str, dataset: xr.Dataset = Depends(deps.dataset), use_cache=False
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
                if ".zmetadata" == key:
                    kv = json.loads(fsmap[key].decode("utf-8"))
                    kv["zarr_consolidated_format"] = 1
                    resp = Response(
                        json.dumps(kv).encode("utf-8"),
                        media_type="application/octet-stream",
                    )
                    fsmap.fs.dircache.clear()
                    del fsmap, kv
                    return resp
                #                return StreamingResponse(
                #                    kerchunk_stream_content(fsmap[key]),
                #                    media_type='application/octet-stream',
                #                )
                resp = StreamingResponse(
                    kerchunk_stream_content(fsmap[key]),
                    media_type="application/octet-stream",
                )
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
