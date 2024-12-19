from typing import Sequence
from dask.array.core import Array as DASKARRAY
from fastapi import APIRouter, Depends, HTTPException, Request
from xpublish.utils.api import JSONResponse, DATASET_ID_ATTR_KEY
import xarray as xr
import json
from xpublish.dependencies import (
    get_dataset,
)  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies
from copy import deepcopy as copy
import cachey
from pystac import Item, Asset, MediaType
from datetime import datetime
import pystac
import json
import fsspec

PARENT_CATALOG = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/catalogs/stac-catalog-eeriecloud.json"
GRIDLOOK = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/apps/gridlook/index.html"
HOST = "https://eerie.cloud.dkrz.de/"
INTAKE_SOURCE = "https://raw.githubusercontent.com/eerie-project/intake_catalogues/refs/heads/main/dkrz/disk/model-output/main.yaml"
JUPYTERLITE = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/apps/jupyterlite/index.html"
HOSTURL = HOST + "datasets"
PROVIDER_DEFAULT = dict(
    name="DKRZ",
    description="The data host of eerie.cloud",
    roles=["host"],
    url="https://dkrz.de",
)
ALTERNATE_KERCHUNK = dict(
    processed={
        "name": "Processed",
        "description": "Server-side on-the-fly rechunked and unifromly encoded data",
    }
)
STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
    "https://stac-extensions.github.io/xarray-assets/v1.0.0/schema.json",
]

XARRAY_ITEM = {"xarray:open_kwargs": dict(consolidated="true")}
EERIE_DESC = """
# Items of the eerie.cloud

DKRZ hosts a data server named ‘eerie.cloud’ for global high resolution Earth System Model simulation output stored at the German Climate Computing Center (DKRZ). This was developped within the EU project EERIE. Eerie.cloud makes use of the python package xpublish. Xpublish is a plugin for xarray (Hoyer, 2023) which is widely used in the Earth System Science community. It serves ESM output formatted as zarr (Miles, 2020) via a RestAPI based on FastAPI. Served in this way, the data imitates cloud-native data (Abernathey, 2021) and features many capabilities of cloud-optimized data.

[Imprint](https://www.dkrz.de/en/about-en/contact/impressum) and
[Privacy Policy](https://www.dkrz.de/en/about-en/contact/en-datenschutzhinweise).
"""

TECHDOC = pystac.Asset(
    href="https://pad.gwdg.de/OZo5HMC4R6iljvZHlo-BzQ#",
    title="Technical documentation",
    media_type=pystac.MediaType.HTML,
    roles=["OVERVIEW"],
)


def create_stac_collection():
    start = datetime(1850, 1, 1)
    end = datetime(2101, 1, 1)
    col = pystac.Collection(
        id="eerie-cloud-all",
        title="ESM data from DKRZ in Zarr format",
        description=EERIE_DESC,
        stac_extensions=STAC_EXTENSIONS,
        href=HOST + "stac-collection-all.json",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([-180, -90, 180, 90]),
            temporal=pystac.TemporalExtent(intervals=[start, end]),
        ),
        keywords=[
            "EERIE",
            "cloud",
            "ICON",
            "NextGEMs",
            "ERA5",
            "IFS",
            "FESOM",
            "NEMO",
            "HadGEM",
        ],
        providers=[pystac.Provider(PROVIDER_DEFAULT)],
        assets=dict(doc=copy(TECHDOC)),
    )
    return col.to_dict()


def xarray_to_stac_item(ds):
    # cube
    cube = dict()
    cube["cube:dimensions"] = dict()
    cube["cube:variables"] = dict()
    for dv in ds.data_vars:
        cube["cube:variables"][dv] = dict(
            type="data",
            dimensions=[*ds[dv].dims],
            unit=ds[dv].attrs.get("units", "Not set"),
            description=ds[dv].attrs.get("long_name", dv),
            attrs=ds[dv].attrs,
        )

    # default
    title = ds.attrs.get("title", "No title")
    description = ds.attrs.get("description", "No description")
    xid = ds.attrs.get("_xpublish_id", "No id")
    keywords = xid.split(".")
    datetimeattr = datetime.now().isoformat()
    start_datetime = ds.attrs.get("time_min", None)
    if start_datetime:
        start_datetime = (
            start_datetime.split(".")[0] + "Z"
        )  # datetime.now().isoformat())
    end_datetime = ds.attrs.get("time_max", None)
    if end_datetime:
        end_datetime = end_datetime.split(".")[0] + "Z"  # datetime.now().isoformat())
    lonmin = -180
    latmin = -90
    lonmax = 180
    latmax = 90
    if start_datetime and end_datetime:
        datetimeattr = None
        cube["cube:dimensions"]["time"] = dict(
            type="temporal", extent=[start_datetime, end_datetime]
        )
    if not "bbox" in ds.attrs and all(a in ds.variables for a in ["lon", "lat"]):
        try:
            if type(ds["lon"].data) != DASKARRAY and type(ds["lat"].data) != DASKARRAY:
                lonmin = ds["lon"].min().values[()]
                latmin = ds["lat"].min().values[()]
                lonmax = ds["lon"].max().values[()]
                latmax = ds["lat"].max().values[()]
        except:
            pass
        ds.attrs["bbox"] = [lonmin, latmin, lonmax, latmax]
    bbox = ds.attrs.get("bbox", [lonmin, latmin, lonmax, latmax])
    geometry = {
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
    cdate = ds.attrs.get("creation_date", datetimeattr)
    providers = [copy(PROVIDER_DEFAULT)]
    creator_inst_id = ds.attrs.get("institution_id", None)
    if not creator_inst_id:
        creator_inst_id = ds.attrs.get("centre", None)
    if creator_inst_id:
        creator_inst = ds.attrs.get("institution", None)
        if not creator_inst:
            creator_inst = ds.attrs.get("centreDescription", None)
        creator = copy(PROVIDER_DEFAULT)
        creator["name"] = creator_inst_id
        creator["description"] = creator_inst
        creator["url"] = HOSTURL + "/" + xid + "/"
        creator["roles"] = ["producer"]
        providers.append(creator)
    description += (
        "\n"
        + "You can use this dataset in xarray from intake.\n\n"
        + "On Levante:\n"
        + "```python\n"
        + "import intake\n"
        + "intake.open_catalog(\n"
        + '    "'
        + INTAKE_SOURCE
        + '"\n'
        + ')["'
        + xid
        + '"].to_dask()\n'
        + "```\n"
        + "Everywhere else:\n"
        + "```python\n"
        + "import intake\n"
        + "intake.open_stac_item(\n"
        + '    "'
        + HOSTURL
        + "/"
        + xid
        + '/stac"\n'
        + ").data.to_dask()\n"
        + "```\n"
    )

    properties = {
        "description": description,
        "title": " ".join(title.split(".")),
        "created": cdate,
        "keywords": keywords,
        "providers": providers,
    }

    if start_datetime:
        properties["start_datetime"] = start_datetime
        properties["end_datetime"] = end_datetime

    # Create a STAC item
    item = Item(
        id=xid,
        geometry=geometry,
        bbox=bbox,
        datetime=datetimeattr,
        properties=properties,
        stac_extensions=STAC_EXTENSIONS,
    )

    store_dataset_dict = dict(store=HOSTURL, dataset=xid + "/zarr")
    var_store_dataset_dict = dict()
    # Add data variables as assets
    for var_name, var_data in ds.data_vars.items():
        var_store_dataset_dict[var_name] = copy(store_dataset_dict)

    extra_fields = {
        "Volume": str(int(ds.nbytes / 1024**3)) + " GB uncompressed",
        "No of data variables": str(len(ds.data_vars)),
    }
    source_enc = ds.encoding.get("source", None)
    extra_fields.update(copy(XARRAY_ITEM))
    if source_enc:
        extra_fields["alternate"] = copy(ALTERNATE_KERCHUNK)
        extra_fields["alternate"]["processed"]["href"] = HOSTURL + "/" + xid + "/zarr"
        extra_fields["alternate"]["processed"][
            "name"
        ] = "Rechunked and uniformly compressed data"
        item.add_asset(
            "data",
            Asset(
                href=HOSTURL + "/" + xid + "/kerchunk",
                media_type=MediaType.ZARR,
                roles=["data"],
                title="Data",
                description="Raw-encoded source data",
                extra_fields=extra_fields,
            ),
        )
    else:
        item.add_asset(
            "data",
            Asset(
                href=HOSTURL + "/" + xid + "/zarr",
                media_type=MediaType.ZARR,
                roles=["data"],
                title="Data",
                description="Accessed binary data is processed on server-side",
                extra_fields=extra_fields,
            ),
        )
    item.add_asset(
        "xarray_view",
        Asset(
            href=HOSTURL + "/" + xid + "/",
            media_type=MediaType.HTML,
            title="Xarray dataset",
            roles=["overview"],
            description="HTML representation of the xarray dataset",
        ),
    )
    item.add_asset(
        "jupyterlite",
        Asset(
            href=JUPYTERLITE,
            media_type=MediaType.HTML,
            title="Jupyterlite access",
            roles=["analysis"],
            description="Web-assembly based analysis platform with access to this item",
        ),
    )
    item.add_asset(
        "gridlook",
        Asset(
            href=GRIDLOOK + "#" + HOSTURL + "/" + xid + "/stac",
            media_type=MediaType.HTML,
            title="Visualization with gridlook",
            roles=["Visualization"],
            description="Visualization with gridlook",
        ),
    )

    itemdict = item.to_dict()
    #    for asset in itemdict["assets"].keys():
    itemdict["links"] = [
        dict(
            rel="DOC",
            href="https://easy.gems.dkrz.de/simulations/EERIE/eerie_data-access_online.html",
            title="Usage of the eerie.cloud",
        )
    ]
    itemdict["links"].append(
        dict(
            rel="collection",
            href=HOST + "/stac-collection-all.json",
            type="application/json",
        )
    )
    #
    # gridlook
    #
    griddict = copy(store_dataset_dict)
    if "icon-esm-er" in xid and "native" in xid:
        griddict[
            "store"
        ] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
        if "atmos" in xid or "land" in xid:
            griddict["dataset"] = "icon_grid_0033_R02B08_G.zarr"
        elif "ocean" in xid:
            griddict["dataset"] = "icon_grid_0016_R02B09_O.zarr"
    if "gr025" in xid:
        griddict[
            "store"
        ] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
        if "ifs-amip" in xid:
            griddict["dataset"] = "gr025_descending.zarr"
        else:
            griddict["dataset"] = "gr025.zarr"
    if "era5" in xid:
        griddict[
            "store"
        ] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
        griddict["dataset"] = "era5.zarr"

    itemdict["default_var"] = list(var_store_dataset_dict.keys())[0]
    itemdict["name"] = title
    itemdict["levels"] = [
        dict(
            name=xid,
            time=copy(store_dataset_dict),
            grid=griddict,
            datasources=var_store_dataset_dict,
        )
    ]
    itemdict["properties"].update(cube)
    for dsatt, dsattval in ds.attrs.items():
        if (
            not dsatt in itemdict["properties"]
            and not dsatt in itemdict
            and not "time" in dsatt.lower()
        ):
            itemdict["properties"][dsatt] = dsattval

    return itemdict


class Stac(Plugin):
    name: str = "stac"
    mapper_dict: dict = {}

    app_router_prefix: str = "/stac"
    app_router_tags: Sequence[str] = ["stac"]

    dataset_router_prefix: str = "/stac"
    dataset_router_tags: Sequence[str] = ["stac"]

    @hookimpl
    def app_router(self, deps: Dependencies):
        """Register an application level router for app level stac catalog"""
        router = APIRouter(prefix=self.app_router_prefix, tags=self.app_router_tags)

        def get_request(request: Request) -> str:
            return request

        @router.get("-collection-all.json", summary="Root stac collection")
        def get_eerie_collection(
            request=Depends(get_request), dataset_ids=Depends(deps.dataset_ids)
        ):
            coldict = create_stac_collection()
            coldict["links"].append(
                dict(rel="parent", href=PARENT_CATALOG, type="application/json")
            )
            coldict["providers"][0] = coldict["providers"][0]["name"]
            dslist = json.load(fsspec.open(HOSTURL).open())
            for item in dslist:
                coldict["links"].append(
                    dict(
                        rel="child",
                        href=HOSTURL + "/" + item + "/stac",
                        type="application/json",
                    )
                )
            return JSONResponse(coldict)

        return router

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )

        @router.get("/")
        @router.get("")
        async def get_stacitem(
            ds: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            cache_key = ds.attrs.get(DATASET_ID_ATTR_KEY, "") + "/" + "stac"
            resp = cache.get(cache_key)
            #            if not all(a in ds.attrs for a in STACKEYS):
            #                raise HTTPException(status_code=404, detail=f"Dataset does not contain all keys needed for a STAC Item")
            if resp is None:
                # try:
                item_dict = xarray_to_stac_item(ds)
                resp = JSONResponse(item_dict)
                # except:
                #    raise HTTPException(status_code=404, detail="Could not create a STAC Item")
                cache.put(cache_key, resp, 99999)
            return resp

        return router
