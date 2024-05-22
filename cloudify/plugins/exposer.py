import os
import fsspec as fs
from xpublish import Plugin, hookimpl, Dependencies
from typing import Sequence
from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse
from fastapi.responses import FileResponse  # , HTMLResponse,RedirectResponse


class FileServe(Plugin):
    name: str = "FileServe"

    app_router_prefix: str = "/static"
    app_router_tags: Sequence[str] = ["get_file"]

    @hookimpl
    def app_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.app_router_prefix, tags=list(self.app_router_tags)
        )

        @router.get("/{staticfile:path}")
        async def get_partial_file(staticfile: str, request: Request):
            trunk = "/work/bm1344/DKRZ"
            file_path = os.path.join(trunk, staticfile)
            if not os.path.isfile(file_path):
                return None

            range_header = request.headers.get("Range")
            if not range_header or not range_header.startswith("bytes="):
                # Handle the case where the Range header is missing or invalid
                # You can return a full file in this case or respond with an error
                return FileResponse(file_path)

            range_values = range_header.replace("bytes=", "").split("-")
            if len(range_values) != 2:
                # Handle an invalid range header
                return FileResponse(file_path)

            start_byte, end_byte = map(int, range_values)
            file_size = os.path.getsize(file_path)

            if end_byte >= file_size:
                end_byte = file_size - 1

            content_length = end_byte - start_byte + 1

            headers = {
                "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
                "Content-Length": str(content_length),
                "Accept-Ranges": "bytes",
            }
            status_code = 206  # Partial Content

            fss = fs.filesystem("file")
            file_handle = fss.open(file_path, "rb")
            file_handle.seek(start_byte)

            def file_generator():
                remaining_bytes = content_length
                chunk_size = 4096
                while remaining_bytes > 0:
                    data = file_handle.read(min(remaining_bytes, chunk_size))
                    if not data:
                        break
                    yield data
                    remaining_bytes -= len(data)

            return StreamingResponse(
                file_generator(),
                headers=headers,
                status_code=status_code,
                media_type="application/octet-stream",
            )

        return router
