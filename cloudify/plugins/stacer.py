from typing import Sequence
from fastapi import APIRouter, Depends, HTTPException, Request
from xpublish.utils.api import JSONResponse
import xarray as xr
import json
from xpublish.dependencies import (
    get_dataset,
)  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies
from copy import deepcopy as copy
from pystac import Item, Asset, MediaType
from datetime import datetime
import pystac
import json
import fsspec

HOST = "https://eerie.cloud.dkrz.de/"
HOSTURL = HOST + "datasets"
PROVIDER_DEFAULT = dict(
    name="DKRZ",
    description="The data host of eerie.cloud",
    roles=["host"],
    url="https://dkrz.de",
)
ALTERNATE_KERCHUNK = dict(
    kerchunk={
        "name": "kerchunk",
        "description": "Accessed binary data is as stored on disk via kerchunk",
    }
)
STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
]

DESC = """
DKRZ hosts a data server named ‘eerie.cloud’ for global
high resolution Earth System Model simulation output
for experiments produced within the EU project EERIE
and stored at the German Climate Computing Center (DKRZ).
Eerie.cloud makes use of the python package xpublish.
Xpublish is a plugin for xarray (Hoyer, 2023) which is widely used in the Earth System Science community.
It serves ESM output formatted as zarr (Miles, 2020) via a RestAPI based on FastAPI.
Served in this way, the data imitates cloud-native data (Abernathey, 2021)
and features many capabilities of cloud-optimized data. """


def create_stac_collection():
    techdoc = pystac.Asset(
        href="https://pad.gwdg.de/OZo5HMC4R6iljvZHlo-BzQ#",
        title="Technical documentation",
        media_type=pystac.MediaType.HTML,
        roles=["OVERVIEW"],
    )
    col = pystac.Collection(
        id="eerie.cloud",
        title="The EERIE cloud data server",
        description=DESC,
        stac_extensions=STAC_EXTENSIONS,
        href=HOST + "stac.json",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([-180, -90, 180, 90]),
            temporal=pystac.TemporalExtent(intervals=[None, None]),
        ),
        keywords=["EERIE", "cloud", "ICON", "IFS", "FESOM", "NEMO", "HadGEM"],
        providers=[pystac.Provider(PROVIDER_DEFAULT)],
        assets=dict(doc=techdoc),
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
    datetimeattr = datetime.now()#.isoformat()
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
    cdate = ds.attrs.get("creation_date", datetimeattr.isoformat())
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
    properties = {
        "description": description,
        "title": title,
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
    if source_enc:
        extra_fields["alternate"] = copy(ALTERNATE_KERCHUNK)
        extra_fields["alternate"]["kerchunk"]["href"] = (
            HOSTURL + "/" + xid + "/kerchunk"
        )
        extra_fields["alternate"]["kerchunk"]["name"] = "Raw data"
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

    itemdict = item.to_dict()
    itemdict["properties"]["datetime"]=datetimeattr.isoformat()
    #    for asset in itemdict["assets"].keys():
    itemdict["links"] = [
        dict(
            rel="DOC",
            href="https://easy.gems.dkrz.de/simulations/EERIE/eerie_data-access_online.html",
            title="Usage of the eerie.cloud",
        )
    ]
    #
    # gridlook
    #
    griddict = copy(store_dataset_dict)
    if "icon-esm-er" in xid and "native" in xid:
        if "atmos" in xid or "land" in xid:
            griddict[
                "store"
            ] = "https://swift.dkrz.de/v1/dkrz_948e7d4bbfbb445fbff5315fc433e36a/nextGEMS/"
            griddict["dataset"] = "grids/ICON_R02B08.zarr"
        elif "ocean" in xid:
            griddict["store"] = "https://eerie.cloud.dkrz.de/datasets/"
            griddict[
                "dataset"
            ] = "icon-esm-er.eerie-control-1950.v20231106.ocean.native.2d_grid/zarr"

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

        @router.get("catalog.json", summary="Root stac catalog")
        def get_root_catalog(
            request=Depends(get_request), dataset_ids=Depends(deps.dataset_ids)
        ):
            coldict = create_stac_collection()
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
        async def get_stacitem(ds: xr.Dataset = Depends(deps.dataset)):
            #            if not all(a in ds.attrs for a in STACKEYS):
            #                raise HTTPException(status_code=404, detail=f"Dataset does not contain all keys needed for a STAC Item")
            item_dict = xarray_to_stac_item(ds)
            return JSONResponse(item_dict)

        return router
