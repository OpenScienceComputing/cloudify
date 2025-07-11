from xpublish import Plugin, hookimpl
import xarray as xr
from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends
from typing import AsyncGenerator
from fastapi import HTTPException
import aiohttp

async def stream_from_another_server(
    url: str
) -> AsyncGenerator[bytes, None]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Failed to fetch data from {url}"
                )
            while True:
                chunk = await response.content.read(1048576)  # 1MB chunks
                if not chunk:
                    break
                yield chunk

class ForwardPlugin(Plugin):
    """
    Forward plugin for xpublish that provides forward-based data access.

    This plugin extends xpublish with endpoints for forward-based data access,
    allowing efficient chunked data access through forward references.
    """

    name: str = "forward"
    source: str = "/work/bm1344/DKRZ/km-scale-cloud-inputs/"
    url_scheme: str = ["host","port","dataset"]
    url_split: str = "_"

    dataset_router_prefix: str = "/forward"
    dataset_router_tags: Sequence[str] = ["forward"]

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )

        @router.api_route("/{key}", methods=["GET", "HEAD"])
        async def get_chunk(
            key: str,
            ds: xr.Dataset = Depends(deps.dataset)
        ):
            return StreamingResponse(
                stream_from_another_server(ds.encoding["source"]+"/kerchunk/"+key),
                media_type="application/octet-stream",
            )
            
        

        return router
