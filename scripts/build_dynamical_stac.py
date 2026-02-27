#!/usr/bin/env python3
"""Build a STAC catalog from all dynamical.org icechunk stores on AWS Open Data.

Discovers datasets automatically by:
  1. Querying awslabs/open-data-registry for dynamical-*.yaml files
  2. Listing each public S3 bucket to find *.icechunk store prefixes
  3. Opening each store, building a STAC item with a copy-paste code snippet
  4. Writing the catalog locally, then publishing to S3 or GitHub Pages

Usage:
    python scripts/build_dynamical_stac.py [options]

    # Dry run — build and save locally only, no upload:
    python scripts/build_dynamical_stac.py --no-upload --output-dir /tmp/stac-out

    # Publish to S3-compatible storage:
    python scripts/build_dynamical_stac.py \\
        --catalog-bucket osc-pub \\
        --catalog-prefix stac/dynamical \\
        --profile osc-pub-r2 \\
        --public-domain r2-pub.openscicomp.io

    # Publish to GitHub Pages (auto-commit; push separately):
    python scripts/build_dynamical_stac.py \\
        --no-upload \\
        --public-domain myorg.github.io/myrepo \\
        --catalog-prefix stac/dynamical \\
        --github-pages /path/to/local/gh-pages-clone

    # Publish to GitHub Pages and auto-push:
    python scripts/build_dynamical_stac.py \\
        --no-upload \\
        --public-domain myorg.github.io/myrepo \\
        --catalog-prefix stac/dynamical \\
        --github-pages /path/to/local/gh-pages-clone \\
        --github-pages-push
"""

import argparse
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import icechunk
import pystac
import requests
import rioxarray  # noqa: F401 — registers .rio accessor for CRS-aware bbox
import s3fs
import xarray as xr
import yaml

from cloudify.stac import build_stac_item_from_icechunk

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thumbnail timeout
# ---------------------------------------------------------------------------

THUMBNAIL_TIMEOUT_SECONDS = 60


class _ThumbnailTimeout(Exception):
    pass


def _thumbnail_timeout_handler(signum, frame):
    raise _ThumbnailTimeout()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REGISTRY_API = (
    "https://api.github.com/repos/awslabs/open-data-registry"
    "/contents/datasets?per_page=300"
)
_REGISTRY_RAW = (
    "https://raw.githubusercontent.com/awslabs/open-data-registry"
    "/main/datasets/{filename}"
)

DYNAMICAL_PROVIDER = pystac.Provider(
    name="dynamical.org",
    roles=["producer", "processor", "host"],
    url="https://dynamical.org",
)


# ---------------------------------------------------------------------------
# Registry discovery
# ---------------------------------------------------------------------------

def fetch_registry_entries() -> list[dict]:
    """Fetch and parse all dynamical-*.yaml entries from the AWS Open Data Registry."""
    log.info("Querying AWS Open Data Registry for dynamical.org datasets ...")
    token = os.environ.get("GITHUB_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(_REGISTRY_API, headers=headers, timeout=30)
    resp.raise_for_status()
    files = [
        f for f in resp.json()
        if f["name"].startswith("dynamical-") and f["name"].endswith(".yaml")
    ]
    log.info("Found %d registry entries: %s", len(files), [f["name"] for f in files])

    entries = []
    for f in files:
        raw = requests.get(_REGISTRY_RAW.format(filename=f["name"]), headers=headers, timeout=30)
        raw.raise_for_status()
        entry = yaml.safe_load(raw.text)
        entry["_filename"] = f["name"]
        entries.append(entry)
    return entries


def bucket_from_entry(entry: dict) -> tuple[str, str]:
    """Return (bucket, region) from a registry YAML entry."""
    for resource in entry.get("Resources", []):
        if resource.get("Type") == "S3 Bucket":
            arn = resource.get("ARN", "")
            bucket = arn.split(":::")[-1]
            region = resource.get("Region", "us-east-1")
            return bucket, region
    raise ValueError(f"No S3 bucket in registry entry: {entry.get('Name')}")


# ---------------------------------------------------------------------------
# Icechunk store discovery
# ---------------------------------------------------------------------------

def discover_icechunk_prefixes(bucket: str, region: str) -> list[str]:
    """List a public S3 bucket and return all icechunk store prefixes found.

    Expects structure:  {bucket}/{dataset-name}/{version}.icechunk/
    Returns prefixes relative to bucket root, e.g.:
        ["noaa-gfs-forecast/v0.2.7.icechunk/", "noaa-gfs-analysis/v0.1.0.icechunk/"]
    """
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": region})
    prefixes = []
    try:
        top_paths = fs.ls(bucket, detail=False)
    except Exception as exc:
        log.warning("Cannot list s3://%s: %s", bucket, exc)
        return prefixes

    for top_path in top_paths:
        try:
            sub_paths = fs.ls(top_path, detail=False)
        except Exception:
            continue
        for sub_path in sub_paths:
            leaf = sub_path.split("/")[-1]
            if leaf.endswith(".icechunk"):
                # strip leading "bucket/" to get the relative prefix
                prefix = sub_path[len(bucket) + 1:].rstrip("/") + "/"
                prefixes.append(prefix)
                log.info("  Found: s3://%s/%s", bucket, prefix)
    return prefixes


# ---------------------------------------------------------------------------
# Opening icechunk stores
# ---------------------------------------------------------------------------

def open_icechunk_store(bucket: str, prefix: str, region: str):
    """Open an anonymous icechunk repo and return (session, ds)."""
    storage = icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region=region, anonymous=True
    )
    repo = icechunk.Repository.open(storage=storage)
    session = repo.readonly_session(branch="main")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Numcodecs codecs are not in the Zarr version 3 specification.*",
        )
        ds = xr.open_zarr(session.store, chunks=None, consolidated=False, zarr_format=3)
    return session, ds


# ---------------------------------------------------------------------------
# Dimension auto-detection
# ---------------------------------------------------------------------------

def detect_temporal_dimension(ds: xr.Dataset) -> str:
    """Return the primary temporal dimension name."""
    for name in ("init_time", "time", "valid_time"):
        if name in ds.dims:
            return name
    # fallback: any dim with 'time' in the name
    for name in ds.dims:
        if "time" in name.lower():
            return name
    return "time"


# ---------------------------------------------------------------------------
# Code snippet
# ---------------------------------------------------------------------------

def xarray_open_snippet(item_id: str, catalog_url: str) -> str:
    """Return a markdown code block showing how to open this item with xarray."""
    return (
        "\n\n## Open in Python\n\n"
        "```python\n"
        "import pystac, xarray as xr\n"
        "import xpystac  # registers xarray backend for icechunk stores\n\n"
        f'catalog = pystac.Catalog.from_file("{catalog_url}")\n'
        f'item = catalog.get_item("{item_id}")\n\n'
        "# The asset key is '{name}@{snapshot_id}'\n"
        "asset_key = next(k for k in item.assets if '@' in k)\n"
        "asset = item.assets[asset_key]\n\n"
        "# xpystac reconstructs the icechunk repo config from storage:schemes\n"
        "ds = xr.open_dataset(asset)\n"
        "```"
    )


# ---------------------------------------------------------------------------
# Thumbnail generation
# ---------------------------------------------------------------------------

def generate_thumbnail(
    session,
    ds: "xr.Dataset",
    item_id: str,
    output_dir: Path,
    temporal_dimension: str,
) -> "Path | None":
    """Generate a PNG temperature map thumbnail for a dataset.

    Re-opens the store with dask chunks of size 1 on non-spatial dims so that
    only the exact slice is fetched from S3, not the full multi-dim zarr chunk.

    Returns the path to the saved PNG, or None if generation fails.
    """
    if "temperature_2m" not in ds:
        log.warning("  No temperature_2m in %s -- skipping thumbnail", item_id)
        return None

    signal.signal(signal.SIGALRM, _thumbnail_timeout_handler)
    signal.alarm(THUMBNAIL_TIMEOUT_SECONDS)
    try:
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        import numpy as np

        # Re-open with dask using chunk size 1 on non-spatial dims.
        # Without this, zarr loads the full stored chunk (which can span many
        # lead_time / ensemble_member slices = many GB) just to extract one slice.
        chunks = {d: 1 for d in ds.dims
                  if d not in ("latitude", "longitude", "x", "y")}
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Numcodecs codecs are not in the Zarr version 3 specification.*",
            )
            ds_lazy = xr.open_zarr(
                session.store, chunks=chunks,
                consolidated=False, zarr_format=3
            )

        da = ds_lazy["temperature_2m"]
        log.info("  [thumb] dims=%s", dict(da.sizes))

        # Select the most recent time slice
        if temporal_dimension == "init_time" and "init_time" in da.dims:
            da = da.isel(init_time=-1, lead_time=0)
        elif temporal_dimension in da.dims:
            da = da.isel({temporal_dimension: -1})

        # Pick one ensemble member to avoid loading all members
        if "ensemble_member" in da.dims:
            da = da.isel(ensemble_member=0)

        # Subsample spatial dims for thumbnail (max ~360x180 points)
        if "latitude" in da.dims and "longitude" in da.dims:
            lat_step = max(1, len(da.latitude) // 180)
            lon_step = max(1, len(da.longitude) // 360)
            da = da.isel(latitude=slice(None, None, lat_step),
                         longitude=slice(None, None, lon_step))
        elif "y" in da.dims and "x" in da.dims:
            y_step = max(1, len(da.y) // 300)
            x_step = max(1, len(da.x) // 500)
            da = da.isel(y=slice(None, None, y_step),
                         x=slice(None, None, x_step))

        log.info("  [thumb] loading %s values...", dict(da.sizes))

        # .compute() with dask fetches only the chunks we need
        data_c = da.compute().values - 273.15
        log.info("  [thumb] loaded shape=%s  min=%.1f max=%.1f",
                 data_c.shape, data_c.min(), data_c.max())

        is_projected = "x" in ds.dims and "y" in ds.dims

        if is_projected:
            proj = ccrs.LambertConformal(
                central_longitude=-97.5, central_latitude=38.5
            )
            fig, ax = plt.subplots(
                figsize=(10, 6), subplot_kw={"projection": proj}
            )
            lons = da["longitude"].compute().values
            lats = da["latitude"].compute().values
            img = ax.pcolormesh(
                lons, lats, data_c,
                cmap="RdBu_r", vmin=-40, vmax=40,
                transform=ccrs.PlateCarree(),
                shading="auto",
            )
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.STATES, linewidth=0.3)
            ax.set_extent([-130, -60, 20, 55], crs=ccrs.PlateCarree())
        else:
            proj = ccrs.PlateCarree()
            fig, ax = plt.subplots(
                figsize=(10, 5), subplot_kw={"projection": proj}
            )
            # Determine lon/lat coordinate names
            lon_name = "longitude" if "longitude" in da.dims else "lon"
            lat_name = "latitude" if "latitude" in da.dims else "lat"
            lons = da[lon_name].values
            lats = da[lat_name].values
            img = ax.pcolormesh(
                lons, lats, data_c,
                cmap="RdBu_r", vmin=-40, vmax=40,
                transform=proj,
                shading="auto",
            )
            ax.set_global()
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)

        plt.colorbar(img, ax=ax, orientation="horizontal", pad=0.04,
                     label="2m Temperature (°C)", shrink=0.7)

        # Build timestamp string for title
        try:
            ts_val = da[temporal_dimension].values if temporal_dimension in da.coords else None
            ts_str = str(np.datetime_as_string(ts_val, unit="h")) if ts_val is not None else ""
        except Exception:
            ts_str = ""

        title = item_id
        if ts_str:
            title += f"  |  {ts_str}"
        ax.set_title(title, fontsize=9)

        thumb_dir = output_dir / "thumbnails"
        thumb_dir.mkdir(parents=True, exist_ok=True)
        out_path = thumb_dir / f"{item_id}.png"
        fig.savefig(out_path, dpi=100, bbox_inches="tight")
        plt.close(fig)
        log.info("  Thumbnail saved: %s", out_path)
        return out_path

    except _ThumbnailTimeout:
        log.warning("  Thumbnail timed out after %ds for %s -- skipping",
                    THUMBNAIL_TIMEOUT_SECONDS, item_id)
        return None
    except Exception as exc:
        log.warning("  Thumbnail generation failed for %s: %s", item_id, exc)
        return None
    finally:
        signal.alarm(0)  # cancel any remaining alarm
        try:
            plt.close("all")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Per-store item building
# ---------------------------------------------------------------------------

def build_item_for_store(
    bucket: str,
    prefix: str,
    region: str,
    entry: dict,
    catalog_url: str,
    output_dir: "Path | None" = None,
    thumbnail_url_base: "str | None" = None,
) -> dict | None:
    """Open one icechunk store and return a STAC item dict, or None on failure."""
    store_uri = f"s3://{bucket}/{prefix}"
    log.info("Opening %s ...", store_uri)
    try:
        session, ds = open_icechunk_store(bucket, prefix, region)
    except Exception as exc:
        log.warning("  Failed to open %s: %s", store_uri, exc)
        return None

    snap = session.snapshot_id
    log.info("  snapshot: %s  dims: %s", snap, dict(ds.sizes))

    # Stable item ID from store path: e.g. "noaa-gfs-forecast-v0-2-7"
    dataset_name = prefix.split("/")[0]          # "noaa-gfs-forecast"
    version_str  = prefix.split("/")[1]          # "v0.2.7.icechunk"
    version_slug = version_str.replace(".icechunk", "").replace(".", "-")
    item_id = f"{dataset_name}-{version_slug}"

    temporal_dim = detect_temporal_dimension(ds)

    description = entry.get("Description", "").strip()
    description += xarray_open_snippet(item_id, catalog_url)

    storage_schemes = {
        f"aws-s3-{bucket}": {
            "type": "aws-s3",
            "bucket": bucket,
            "region": region,
            "anonymous": True,
        }
    }

    try:
        item_dict = build_stac_item_from_icechunk(
            ds,
            item_id=item_id,
            icechunk_href=store_uri,
            snapshot_id=snap,
            storage_schemes=storage_schemes,
            title=entry.get("Name", item_id),
            description=description,
            providers=[DYNAMICAL_PROVIDER],
            virtual=False,
            temporal_dimension=temporal_dim,
        )
    except Exception as exc:
        log.warning("  Failed to build STAC item for %s: %s", store_uri, exc)
        return None

    log.info("  Built item: %s  bbox=%s", item_id, item_dict["bbox"])

    if output_dir and thumbnail_url_base:
        thumb_path = generate_thumbnail(session, ds, item_id, output_dir, temporal_dim)
        if thumb_path:
            item_dict["assets"]["thumbnail"] = {
                "href": f"{thumbnail_url_base}/{item_id}.png",
                "type": "image/png",
                "roles": ["thumbnail"],
                "title": "Latest 2m temperature map",
            }

    return item_dict


# ---------------------------------------------------------------------------
# Catalog assembly
# ---------------------------------------------------------------------------

def build_catalog(
    catalog_bucket: str,
    catalog_prefix: str,
    public_domain: str,
    output_dir: "Path | None" = None,
) -> "tuple[pystac.Catalog, str]":
    """Discover all stores, build items, return (catalog, catalog_url)."""
    catalog_url = f"https://{public_domain}/{catalog_prefix}/catalog.json"
    thumbnail_url_base = (
        f"https://{public_domain}/{catalog_prefix}/thumbnails"
        if output_dir else None
    )

    catalog = pystac.Catalog(
        id="dynamical-org-icechunk",
        description=(
            "Weather forecast and analysis datasets from dynamical.org, "
            "stored as Icechunk repositories on AWS S3. "
            "All items can be opened directly with xarray via xpystac."
        ),
        catalog_type=pystac.CatalogType.SELF_CONTAINED,
    )

    entries = fetch_registry_entries()

    for entry in entries:
        try:
            bucket, region = bucket_from_entry(entry)
        except ValueError as exc:
            log.warning("%s — skipping", exc)
            continue

        log.info("\nScanning s3://%s (%s) ...", bucket, entry.get("Name", "?"))
        prefixes = discover_icechunk_prefixes(bucket, region)
        if not prefixes:
            log.warning("  No icechunk stores found in s3://%s", bucket)
            continue

        for prefix in prefixes:
            item_dict = build_item_for_store(
                bucket, prefix, region, entry, catalog_url,
                output_dir=output_dir,
                thumbnail_url_base=thumbnail_url_base,
            )
            if item_dict:
                catalog.add_item(pystac.Item.from_dict(item_dict))

    return catalog, catalog_url


# ---------------------------------------------------------------------------
# Local save + S3 upload
# ---------------------------------------------------------------------------

def save_locally(catalog: pystac.Catalog, output_dir: Path) -> None:
    catalog.normalize_hrefs(str(output_dir))
    catalog.save()
    log.info("\nCatalog saved to: %s", output_dir)
    for f in sorted(output_dir.rglob("*.json")):
        log.info("  %s  (%d bytes)", f.relative_to(output_dir), f.stat().st_size)


def write_geoparquet(catalog: pystac.Catalog, output_dir: Path) -> Path:
    """Write all catalog items to a stac-geoparquet file readable by rustac."""
    import asyncio
    import rustac.geoparquet

    items = [item.to_dict() for item in catalog.get_items()]
    out_path = output_dir / "catalog.parquet"

    async def _write():
        async with rustac.geoparquet.geoparquet_writer(items, str(out_path)):
            pass  # all items passed at open time

    asyncio.run(_write())
    log.info("GeoParquet written: %s (%d bytes)", out_path.name, out_path.stat().st_size)
    return out_path


def upload_to_s3(
    output_dir: Path,
    catalog_bucket: str,
    catalog_prefix: str,
    profile: str,
) -> None:
    fs = s3fs.S3FileSystem(profile=profile)
    log.info("\nUploading to s3://%s/%s ...", catalog_bucket, catalog_prefix)
    for pattern in ("**/*.json", "*.parquet", "thumbnails/*.png"):
        for local_file in sorted(output_dir.glob(pattern)):
            rel = local_file.relative_to(output_dir)
            s3_dest = f"{catalog_bucket}/{catalog_prefix}/{rel}"
            fs.put(str(local_file), s3_dest)
            log.info("  %s  →  s3://%s", rel, s3_dest)


def publish_to_github_pages(
    output_dir: Path,
    pages_dir: Path,
    catalog_prefix: str,
    auto_push: bool = False,
) -> None:
    """Copy catalog JSON into a local GitHub Pages repo and commit.

    Files from output_dir are copied to pages_dir/catalog_prefix/, preserving
    the relative directory structure.  A git commit is then made in pages_dir.
    Pass auto_push=True to also run `git push`.
    """
    dest_dir = pages_dir / catalog_prefix
    dest_dir.mkdir(parents=True, exist_ok=True)

    log.info("\nCopying catalog to GitHub Pages repo at %s ...", pages_dir)
    for local_file in sorted(output_dir.rglob("*.json")):
        rel = local_file.relative_to(output_dir)
        dest = dest_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_file, dest)
        log.info("  %s  →  %s", rel, dest.relative_to(pages_dir))

    subprocess.run(["git", "add", str(dest_dir)], cwd=pages_dir, check=True)

    # Check whether there is anything new to commit
    changed = subprocess.run(
        ["git", "diff", "--cached", "--quiet"], cwd=pages_dir
    ).returncode != 0

    if not changed:
        log.info("No changes to commit in %s", pages_dir)
        return

    subprocess.run(
        ["git", "commit", "-m", "Update dynamical.org STAC catalog"],
        cwd=pages_dir,
        check=True,
    )
    log.info("Committed catalog in %s", pages_dir)

    if auto_push:
        subprocess.run(["git", "push"], cwd=pages_dir, check=True)
        log.info("Pushed to remote.")
    else:
        log.info("Run 'git push' in %s to publish.", pages_dir)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--catalog-bucket",  default="osc-pub")
    parser.add_argument("--catalog-prefix",  default="stac/dynamical")
    parser.add_argument("--profile",         default="osc-pub-r2",
                        help="AWS profile with write credentials for catalog bucket")
    parser.add_argument("--public-domain",   default="r2-pub.openscicomp.io",
                        help="Public read domain for the catalog bucket")
    parser.add_argument("--output-dir",      type=Path, default=None,
                        help="Local dir to write JSON (default: temp dir)")
    parser.add_argument("--no-upload",       action="store_true",
                        help="Skip S3 upload (build and save locally only)")
    parser.add_argument("--geoparquet",      action="store_true",
                        help="Generate catalog.parquet (stac-geoparquet) alongside JSON")
    parser.add_argument("--github-pages",    type=Path, default=None,
                        metavar="DIR",
                        help="Local path to a GitHub Pages git clone. "
                             "Catalog files are copied to DIR/catalog-prefix/ "
                             "and auto-committed. Set --public-domain to the "
                             "GitHub Pages hostname (e.g. myorg.github.io/myrepo).")
    parser.add_argument("--github-pages-push", action="store_true",
                        help="Auto git-push after committing to the GitHub Pages repo.")
    parser.add_argument("--thumbnails",      action="store_true",
                        help="Generate temperature thumbnails and upload to R2")
    parser.add_argument("-v", "--verbose",   action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    output_dir = args.output_dir or Path(tempfile.mkdtemp(prefix="dynamical-stac-"))
    output_dir.mkdir(parents=True, exist_ok=True)

    catalog, catalog_url = build_catalog(
        catalog_bucket=args.catalog_bucket,
        catalog_prefix=args.catalog_prefix,
        public_domain=args.public_domain,
        output_dir=output_dir if args.thumbnails else None,
    )

    n_items = len(list(catalog.get_items()))
    if n_items == 0:
        log.error("No STAC items were built — aborting.")
        sys.exit(1)
    log.info("\nBuilt %d STAC items.", n_items)

    save_locally(catalog, output_dir)

    if args.geoparquet:
        write_geoparquet(catalog, output_dir)

    if args.no_upload:
        log.info("--no-upload set, skipping S3 upload.")
    else:
        upload_to_s3(output_dir, args.catalog_bucket, args.catalog_prefix, args.profile)

    if args.github_pages:
        publish_to_github_pages(
            output_dir,
            args.github_pages,
            args.catalog_prefix,
            auto_push=args.github_pages_push,
        )

    browser_url = (
        "https://radiantearth.github.io/stac-browser/#/external/"
        + catalog_url
    )
    print(f"\nCatalog URL:  {catalog_url}")
    print(f"STAC Browser: {browser_url}")


if __name__ == "__main__":
    main()
