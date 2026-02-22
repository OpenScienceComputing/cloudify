"""
Generic STAC builder functions for cloud-hosted model datasets.

No xpublish dependency — works standalone with xarray + pystac.
"""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pystac
import xarray as xr

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str | Path) -> dict:
    """Load a JSON config file and return it as a dict."""
    with open(config_path) as f:
        return json.load(f)


def merge_configs(*configs: dict) -> dict:
    """Deep-merge dicts; later dicts take precedence over earlier ones."""
    result: dict = {}
    for cfg in configs:
        _deep_merge(result, cfg)
    return result


def _deep_merge(base: dict, override: dict) -> dict:
    """Mutate *base* by deep-merging *override* into it."""
    for key, val in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(val, dict):
            _deep_merge(base[key], val)
        else:
            base[key] = deepcopy(val)
    return base


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_temporal_extent(ds: xr.Dataset) -> tuple[str | None, str | None]:
    """Return (time_min_iso, time_max_iso) with 'Z' suffix, or (None, None)."""
    # Try xstac first (optional dependency)
    try:
        import xstac  # noqa: F401
        # xstac doesn't directly expose min/max – fall through to manual
    except ImportError:
        pass

    if "time" not in ds.variables:
        return None, None

    try:
        time_min = str(ds["time"].min().values[()]).split(".")[0] + "Z"
        time_max = str(ds["time"].max().values[()]).split(".")[0] + "Z"
        return time_min, time_max
    except Exception:
        return None, None


def extract_spatial_extent(ds: xr.Dataset) -> list[float]:
    """Return [lonmin, latmin, lonmax, latmax] from dataset coordinates.

    Fixes the dead-code early-return bug in the original get_bbox().
    Falls back to global extent [-180, -90, 180, 90] if coordinates are absent.
    """
    lonmin, latmin, lonmax, latmax = -180.0, -90.0, 180.0, 90.0

    lon_names = [n for n in ["lon", "longitude"] if n in ds.variables]
    lat_names = [n for n in ["lat", "latitude"] if n in ds.variables]

    if lon_names and lat_names:
        try:
            ds_rc = ds.reset_coords()
            lonmin = float(ds_rc[lon_names[0]].min().values[()])
            latmin = float(ds_rc[lat_names[0]].min().values[()])
            lonmax = float(ds_rc[lon_names[0]].max().values[()])
            latmax = float(ds_rc[lat_names[0]].max().values[()])
        except Exception:
            pass

    return [lonmin, latmin, lonmax, latmax]


def extract_spatial_extent_rio(ds: xr.Dataset) -> list[float]:
    """Return [lonmin, latmin, lonmax, latmax] in WGS84 using rioxarray.

    Always reprojects to EPSG:4326 so the result is valid for a STAC bbox
    regardless of the dataset's native CRS (e.g. Lambert Conformal for HRRR,
    plain lon/lat for GFS).  Falls back to extract_spatial_extent() if
    rioxarray is not installed or the dataset has no CRS information.
    """
    try:
        import rioxarray  # noqa: F401
        # transform_bounds reprojects from the native CRS to the target CRS.
        # Use the first data variable so we get a DataArray (more reliable
        # CRS handling than the Dataset accessor in older rioxarray versions).
        first_var = next(iter(ds.data_vars))
        lonmin, latmin, lonmax, latmax = ds[first_var].rio.transform_bounds("EPSG:4326")
        # Clamp to just inside valid WGS84 range. Two reasons:
        # 1. transform_bounds() returns pixel-edge values (e.g. -90.125) that
        #    STAC Browser rejects as invalid geometry.
        # 2. A polygon with edges exactly at ±180° lon coincides with Leaflet's
        #    tile seam and renders as two vertical lines instead of a filled
        #    rectangle. Using ±179.9/±89.9 avoids the antimeridian artifact.
        lonmin = max(-179.9, lonmin)
        latmin = max(-89.9, latmin)
        lonmax = min(179.9, lonmax)
        latmax = min(89.9, latmax)
        return [lonmin, latmin, lonmax, latmax]
    except Exception:
        return extract_spatial_extent(ds)


def build_datacube_extension(
    ds: xr.Dataset,
    time_min: str | None = None,
    time_max: str | None = None,
) -> dict:
    """Return a dict with cube:dimensions and cube:variables."""
    cube: dict[str, Any] = {"cube:dimensions": {}, "cube:variables": {}}

    for dv in ds.data_vars:
        cube["cube:variables"][dv] = {
            "type": "data",
            "dimensions": list(ds[dv].dims),
            "unit": ds[dv].attrs.get("units", "not set"),
            "description": ds[dv].attrs.get("long_name", str(dv)),
        }

    if time_min and time_max:
        cube["cube:dimensions"]["time"] = {
            "type": "temporal",
            "extent": [time_min, time_max],
        }

    return cube


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------

def make_json_serializable(obj: Any) -> Any:
    """Recursively convert non-JSON-serializable values to serializable ones."""
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(v) for v in obj]
    if isinstance(obj, bytes):
        return obj.decode(errors="ignore")
    try:
        json.dumps(obj)
        return obj
    except (TypeError, OverflowError):
        return str(obj)


# ---------------------------------------------------------------------------
# Geometry helper
# ---------------------------------------------------------------------------

def _bbox_to_geometry(bbox: list[float]) -> dict:
    lonmin, latmin, lonmax, latmax = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [lonmin, latmin],
                [lonmin, latmax],
                [lonmax, latmax],
                [lonmax, latmin],
                [lonmin, latmin],
            ]
        ],
    }


# ---------------------------------------------------------------------------
# STAC item construction
# ---------------------------------------------------------------------------

_DEFAULT_OPEN_KWARGS = {"engine": "zarr", "chunks": "auto", "consolidated": True}

_GENERIC_ASSET_DEFAULTS = {
    "title": "Data store",
    "description": "Cloud-optimized data",
    "media_type": pystac.MediaType.ZARR,
    "roles": ["data"],
    "open_kwargs": _DEFAULT_OPEN_KWARGS,
    "storage_options": {},
}


def build_stac_item(
    ds: xr.Dataset,
    item_id: str,
    config: dict,
    assets: dict[str, str] | None = None,
) -> dict:
    """Build a STAC item dict from an xarray Dataset.

    Parameters
    ----------
    ds:       The source dataset.
    item_id:  STAC item ID string.
    config:   Merged config dict (from load_config / merge_configs).
    assets:   Mapping of asset_name → URL string.  Asset metadata comes
              from config["assets"][asset_name]; unknown names get generic
              defaults.

    Returns
    -------
    A JSON-serializable STAC item dict.
    """
    stac_extensions = list(config.get("stac_extensions", []))

    # --- temporal extent ---------------------------------------------------
    time_min, time_max = extract_temporal_extent(ds)
    # Allow config to override
    ip = config.get("item_properties", {})
    if ip.get("start_datetime"):
        time_min = ip["start_datetime"]
    if ip.get("end_datetime"):
        time_max = ip["end_datetime"]

    # --- spatial extent ----------------------------------------------------
    bbox = extract_spatial_extent(ds)

    # --- datacube extension ------------------------------------------------
    cube = build_datacube_extension(ds, time_min, time_max)

    # --- properties --------------------------------------------------------
    title = ip.get("title") or ds.attrs.get("title", item_id)
    description = ip.get("description") or ds.attrs.get("description", "")

    properties: dict[str, Any] = {
        "title": title,
        "description": description,
        "created": ds.attrs.get("creation_date", datetime.now().isoformat()),
        "variables": list(cube["cube:variables"].keys()),
        **cube,
        **{k: v for k, v in ip.items() if k not in ("title", "description")},
    }

    # datetime handling
    dt_attr: datetime | None = datetime.now()
    if time_min is not None:
        dt_attr = None
        properties["start_datetime"] = time_min
        properties["end_datetime"] = time_max

    # --- build pystac Item -------------------------------------------------
    item = pystac.Item(
        id=item_id,
        geometry=_bbox_to_geometry(bbox),
        bbox=bbox,
        datetime=dt_attr,
        properties=properties,
        stac_extensions=stac_extensions,
    )

    # --- assets ------------------------------------------------------------
    assets = assets or {}
    config_assets = config.get("assets", {})

    for asset_name, href in assets.items():
        asset_cfg = deepcopy(config_assets.get(asset_name, _GENERIC_ASSET_DEFAULTS))
        open_kwargs = asset_cfg.pop("open_kwargs", _DEFAULT_OPEN_KWARGS)
        storage_options = asset_cfg.pop("storage_options", {})
        media_type = asset_cfg.pop("media_type", pystac.MediaType.ZARR)
        roles = asset_cfg.pop("roles", ["data"])
        title_a = asset_cfg.pop("title", "Data store")
        description_a = asset_cfg.pop("description", "")

        extra_fields: dict = {}
        if open_kwargs:
            extra_fields["xarray:open_kwargs"] = open_kwargs
        if storage_options:
            extra_fields["xarray:storage_options"] = storage_options
        extra_fields.update(asset_cfg)  # any remaining fields go through

        item.add_asset(
            asset_name,
            pystac.Asset(
                href=str(href),
                media_type=media_type,
                roles=roles,
                title=title_a,
                description=description_a,
                extra_fields=extra_fields,
            ),
        )

    # --- links from config -------------------------------------------------
    for link in config.get("links", []):
        item.add_link(pystac.Link(**link))

    item_dict = item.to_dict()
    return make_json_serializable(item_dict)


# ---------------------------------------------------------------------------
# Icechunk STAC item construction
# ---------------------------------------------------------------------------

#: Media type for icechunk repos (proposed — no official registration yet).
#: Discussion: https://earthmover-community.slack.com/archives/C07NQCBSTB7/p1756918042834049
ICECHUNK_MEDIA_TYPE = "application/vnd.zarr+icechunk"

#: STAC extensions required for a well-formed icechunk item.
ICECHUNK_STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    "https://stac-extensions.github.io/virtual-assets/v1.0.0/schema.json",
    "https://stac-extensions.github.io/zarr/v1.1.0/schema.json",
    "https://stac-extensions.github.io/version/v1.2.0/schema.json",
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
]


def _detect_xy_dims(ds: xr.Dataset) -> tuple[str, str]:
    """Best-effort detection of x/y dimension names for xstac."""
    for x in ["lon", "longitude", "x"]:
        for y in ["lat", "latitude", "y"]:
            if x in ds.dims and y in ds.dims:
                return x, y
    return "lon", "lat"   # xstac default; will be ignored if dims absent


def build_stac_item_from_icechunk(
    ds: xr.Dataset,
    item_id: str,
    icechunk_href: str,
    snapshot_id: str,
    storage_schemes: dict,
    *,
    asset_name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    providers: list[pystac.Provider] | None = None,
    virtual: bool = True,
    virtual_hrefs: list[str] | None = None,
    extra_stac_extensions: list[str] | None = None,
    temporal_dimension: str = "time",
    x_dimension: str | None = None,
    y_dimension: str | None = None,
    reference_system: int = 4326,
) -> dict:
    """Build a STAC item dict for an icechunk (or virtual icechunk) repository.

    Parameters
    ----------
    ds:
        xarray Dataset opened from the icechunk store
        (``xr.open_zarr(session.store, consolidated=False, zarr_format=3)``).
    item_id:
        STAC item ID.
    icechunk_href:
        S3 (or other) URI of the icechunk repository root,
        e.g. ``"s3://my-bucket/path/to/repo/"``.
    snapshot_id:
        Icechunk snapshot ID string (upper-case alphanumeric).  Stored as
        ``"version"`` in the asset extra_fields so that xpystac can use it
        to open the correct session via ``repo.readonly_session()``.
    storage_schemes:
        Dict describing the storage backend, keyed by a short scheme name.
        Stored at the top-level item ``extra_fields`` (NOT inside
        ``properties``) so that ``xpystac`` / ``xr.open_dataset(asset)``
        can find it.  Example::

            {
                "aws-s3-my-bucket": {
                    "type": "aws-s3",
                    "platform": "https://{bucket}.s3.{region}.amazonaws.com",
                    "bucket": "my-bucket",
                    "region": "us-west-2",
                    "anonymous": True,
                }
            }

    asset_name:
        Key for the asset in the STAC item.  Defaults to
        ``"{item_id_base}@{snapshot_id}"``.
    title:
        Item title.  Falls back to ``ds.attrs["title"]`` then ``item_id``.
    description:
        Item description.  Falls back to ``ds.attrs.get("description", "")``.
    providers:
        List of ``pystac.Provider`` objects.
    virtual:
        ``True`` (default) for virtual-chunk repos where chunk data lives in
        external files.  Adds ``"virtual"`` to asset roles and ``"vrt:hrefs"``
        to the asset extra_fields (required by xpystac for round-trip open).
    virtual_hrefs:
        List of S3 URIs where the virtual chunk data lives, e.g.
        ``["s3://my-bucket/source-data/"]``.  Required when ``virtual=True``
        for the xpystac round-trip to work.  Each href becomes a separate
        asset in the item that the ``"vrt:hrefs"`` field points to.
    extra_stac_extensions:
        Additional STAC extension URLs to include beyond the icechunk defaults.
    temporal_dimension:
        Name of the time dimension passed to xstac (default ``"time"``).
    x_dimension:
        Name of the x/longitude dimension for xstac.  Auto-detected if None.
    y_dimension:
        Name of the y/latitude dimension for xstac.  Auto-detected if None.
    reference_system:
        EPSG code for xstac (default 4326).

    Returns
    -------
    A JSON-serializable STAC item dict.
    """
    # --- spatial extent ----------------------------------------------------
    bbox = extract_spatial_extent_rio(ds)

    # --- temporal extent ---------------------------------------------------
    start_dt = end_dt = None
    if temporal_dimension in ds.variables:
        try:
            start_dt = datetime.fromisoformat(
                str(ds[temporal_dimension].min().values[()]).split(".")[0]
            )
            end_dt = datetime.fromisoformat(
                str(ds[temporal_dimension].max().values[()]).split(".")[0]
            )
        except Exception:
            pass

    # --- metadata ----------------------------------------------------------
    _title = title or ds.attrs.get("title", item_id)
    _description = description or ds.attrs.get("description", "")
    _providers = [p.to_dict() for p in (providers or [])]

    properties: dict[str, Any] = {
        "title": _title,
        "description": _description,
    }
    if _providers:
        properties["providers"] = _providers

    # --- STAC extensions ---------------------------------------------------
    extensions = list(ICECHUNK_STAC_EXTENSIONS)
    for ext in (extra_stac_extensions or []):
        if ext not in extensions:
            extensions.append(ext)

    # --- build template item -----------------------------------------------
    template = pystac.Item(
        id=item_id,
        geometry=_bbox_to_geometry(bbox),
        bbox=bbox,
        datetime=None,
        start_datetime=start_dt,
        end_datetime=end_dt,
        properties=properties,
        stac_extensions=extensions,
    )

    # storage:schemes must live at the top-level item extra_fields, NOT in
    # properties — that's where xpystac._icechunk.read_icechunk() looks for it.
    template.extra_fields["storage:schemes"] = storage_schemes

    # --- fill datacube extension via xstac ---------------------------------
    x_dim, y_dim = x_dimension, y_dimension
    if x_dim is None or y_dim is None:
        _x, _y = _detect_xy_dims(ds)
        x_dim = x_dim or _x
        y_dim = y_dim or _y

    try:
        import xstac
        item = xstac.xarray_to_stac(
            ds,
            template,
            temporal_dimension=temporal_dimension,
            x_dimension=x_dim,
            y_dimension=y_dim,
            reference_system=reference_system,
            validate=False,
        )
    except Exception:
        # xstac not installed or failed — fall back to manual datacube build
        time_min = start_dt.isoformat() + "Z" if start_dt else None
        time_max = end_dt.isoformat() + "Z" if end_dt else None
        cube = build_datacube_extension(ds, time_min, time_max)
        template.properties.update(cube)
        item = template

    # --- asset key ---------------------------------------------------------
    base_name = asset_name or item_id.split("-")[0]
    asset_key = f"{base_name}@{snapshot_id}"

    # --- storage:refs ------------------------------------------------------
    storage_refs = list(storage_schemes.keys())

    asset_roles = ["data", "references", "latest-version"]
    if virtual:
        asset_roles.append("virtual")

    # --- virtual source assets ---------------------------------------------
    # xpystac.read_icechunk() looks for "vrt:hrefs" in the primary asset —
    # a list of {"key": <asset_key>} dicts — then fetches that asset's href
    # as the VirtualChunkContainer source URL.
    vrt_hrefs = []
    for i, vh in enumerate(virtual_hrefs or []):
        src_key = f"{base_name}-virtual-source" if len(virtual_hrefs or []) == 1 \
                  else f"{base_name}-virtual-source-{i}"
        item.add_asset(
            src_key,
            pystac.Asset(
                href=vh,
                roles=["virtual-source"],
                extra_fields={"storage:refs": storage_refs},
            ),
        )
        vrt_hrefs.append({"key": src_key})

    # --- primary icechunk repo asset ---------------------------------------
    primary_extra: dict[str, Any] = {
        "zarr:consolidated": False,
        "zarr:zarr_format": 3,
        "icechunk:snapshot_id": snapshot_id,
        # "version" is what xpystac uses to call repo.readonly_session():
        # it tries branch → tag → snapshot_id in that order.
        "version": snapshot_id,
        "storage:refs": storage_refs,
    }
    if vrt_hrefs:
        primary_extra["vrt:hrefs"] = vrt_hrefs

    item.add_asset(
        asset_key,
        pystac.Asset(
            href=icechunk_href,
            title=_title,
            media_type=ICECHUNK_MEDIA_TYPE,
            roles=asset_roles,
            extra_fields=primary_extra,
        ),
    )

    item_dict = item.to_dict()
    return make_json_serializable(item_dict)


# ---------------------------------------------------------------------------
# STAC collection construction
# ---------------------------------------------------------------------------

def build_stac_collection(config: dict) -> dict:
    """Build a pystac.Collection dict from config.

    Uses config["collection"] for collection-level fields and
    config["providers"] for provider list.
    """
    col_cfg = config.get("collection", {})
    providers_cfg = config.get("providers", [])

    providers = [pystac.Provider(**p) for p in providers_cfg]

    start = datetime(1850, 1, 1)
    end = datetime(2101, 1, 1)
    extent = pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent(intervals=[[start, end]]),
    )

    collection = pystac.Collection(
        id=col_cfg.get("id", "my-collection"),
        title=col_cfg.get("title", "My Collection"),
        description=col_cfg.get("description", ""),
        stac_extensions=list(config.get("stac_extensions", [])),
        extent=extent,
        keywords=col_cfg.get("keywords", []),
        license=col_cfg.get("license", "other"),
        providers=providers,
    )

    col_dict = collection.to_dict()

    parent_catalog = col_cfg.get("parent_catalog")
    if parent_catalog:
        col_dict.setdefault("links", []).append(
            {"rel": "parent", "href": parent_catalog, "type": "application/json"}
        )

    return make_json_serializable(col_dict)
