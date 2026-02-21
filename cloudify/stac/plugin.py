"""
xpublish Plugin for STAC item/collection generation.

Thin wrapper around cloudify.stac.builder — no EERIE-specific logic here.
"""
from __future__ import annotations

from pathlib import Path
from typing import Sequence

import cachey
import pystac
import requests
import xarray as xr
from fastapi import APIRouter, Depends, Request
from xpublish import Dependencies, Plugin, hookimpl
from xpublish.utils.api import DATASET_ID_ATTR_KEY, JSONResponse

from cloudify.stac.builder import (
    build_stac_collection,
    build_stac_item,
    build_stac_item_from_icechunk,
    load_config,
    make_json_serializable,
    merge_configs,
)

# ---------------------------------------------------------------------------
# Resolve bundled default config path
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
_DEFAULT_CONFIG = _HERE / "configs" / "default.json"


def _load_default_config() -> dict:
    return load_config(_DEFAULT_CONFIG)


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class Stac(Plugin):
    """xpublish plugin that serves STAC items and collections.

    Parameters
    ----------
    config_path:
        Path to a JSON config file.  Merged on top of the bundled
        ``default.json``; institution-specific overrides live here.
    config:
        In-memory config dict.  Merged last (highest precedence).
    """

    name: str = "stac"

    config_path: str | None = None
    config: dict = {}

    app_router_prefix: str = "/stac"
    app_router_tags: Sequence[str] = ["stac"]

    dataset_router_prefix: str = "/stac"
    dataset_router_tags: Sequence[str] = ["stac"]

    # ------------------------------------------------------------------
    # Internal: build the merged config for a request
    # ------------------------------------------------------------------

    def _merged_config(self) -> dict:
        cfg = _load_default_config()
        if self.config_path:
            cfg = merge_configs(cfg, load_config(self.config_path))
        if self.config:
            cfg = merge_configs(cfg, self.config)
        return cfg

    # ------------------------------------------------------------------
    # Application-level router  →  /stac-collection-all.json
    # ------------------------------------------------------------------

    @hookimpl
    def app_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.app_router_prefix, tags=list(self.app_router_tags)
        )

        plugin_self = self  # capture for closure

        @router.get("-collection-all.json", summary="Root STAC collection")
        def get_collection(
            request: Request,
            dataset_ids=Depends(deps.dataset_ids),
        ):
            cfg = plugin_self._merged_config()
            col_dict = build_stac_collection(cfg)

            # Build host URL from request
            base_url = str(request.base_url).rstrip("/")
            host_url = f"{base_url}/datasets"

            # Add child links for each served dataset
            try:
                dslist = eval(requests.get(host_url).text)
            except Exception:
                dslist = list(dataset_ids()) if callable(dataset_ids) else []

            for ds_id in dslist:
                col_dict.setdefault("links", []).append(
                    {
                        "rel": "child",
                        "href": f"{host_url}/{ds_id}/stac",
                        "title": ds_id,
                        "type": "application/json",
                    }
                )

            return JSONResponse(col_dict)

        return router

    # ------------------------------------------------------------------
    # Dataset-level router  →  /datasets/{id}/stac
    # ------------------------------------------------------------------

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )

        plugin_self = self  # capture for closure

        @router.get("/")
        @router.get("")
        async def get_stac_item(
            request: Request,
            ds: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            item_id = ds.attrs.get(DATASET_ID_ATTR_KEY, "unknown")
            cache_key = item_id + "/stac"
            resp = cache.get(cache_key)

            if resp is None:
                cfg = plugin_self._merged_config()
                base_url = str(request.base_url).rstrip("/")

                icechunk_href = ds.attrs.get("_icechunk_href")
                snapshot_id = ds.attrs.get("_icechunk_snapshot_id")

                if icechunk_href and snapshot_id:
                    # Dataset was opened from an icechunk store — point the
                    # STAC asset at the original repo, not the xpublish URL.
                    storage_schemes = cfg.get("storage_schemes", {})
                    item_dict = build_stac_item_from_icechunk(
                        ds,
                        item_id=item_id,
                        icechunk_href=icechunk_href,
                        snapshot_id=snapshot_id,
                        storage_schemes=storage_schemes,
                    )
                else:
                    # Standard zarr dataset — point asset at xpublish zarr endpoint
                    zarr_href = f"{base_url}/datasets/{item_id}/zarr"
                    assets = {"data": zarr_href}

                    # If the dataset has a disk/cloud source, expose it too
                    disk_source = ds.encoding.get("source")
                    if disk_source:
                        assets["dkrz-disk"] = disk_source

                    item_dict = build_stac_item(ds, item_id, cfg, assets=assets)

                item_dict = make_json_serializable(item_dict)

                resp = JSONResponse(item_dict)
                cache.put(cache_key, resp, 99999)

            return resp

        return router
