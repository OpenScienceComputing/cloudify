from fastapi import APIRouter, Depends, Response, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pathlib import Path

from ..dependencies import get_dataset_ids

dataset_collection_router = APIRouter()

# Path to your on-disk Zarr store
ZARR_STORE = Path("tree.zarr")


@dataset_collection_router.get("/datasets")
def get_dataset_collection_keys(ids: list = Depends(get_dataset_ids)) -> list[str]:
    """Return all the currently known Dataset IDs"""
    return ids


@dataset_collection_router.get("/datasets/.zgroup")
def get_zgroup() -> Response:
    """Return the Zarr .zgroup metadata for the dataset collection"""
    zgroup_path = ZARR_STORE / ".zgroup"
    if not zgroup_path.exists():
        raise HTTPException(status_code=404, detail=".zgroup not found on disk")
    return FileResponse(zgroup_path, media_type="application/json")


@dataset_collection_router.get("/datasets/.zmetadata")
def get_zmetadata() -> FileResponse:
    zmetadata_path = ZARR_STORE / ".zmetadata"
    if not zmetadata_path.exists():
        raise HTTPException(status_code=404, detail=".zmetadata not found on disk")
    return FileResponse(zmetadata_path, media_type="application/json")
