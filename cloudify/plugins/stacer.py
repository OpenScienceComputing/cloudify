from typing import Sequence
from fastapi import APIRouter, Depends, HTTPException, Request
from xpublish.utils.api import JSONResponse
import xarray as xr 
import json
from xpublish.dependencies import get_dataset  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies
from copy import deepcopy as copy
from pystac import Item, Asset, MediaType
from datetime import datetime

HOSTURL="https://eerie.cloud.dkrz.de/datasets/"
PROVIDER_DEFAULT=dict(
        name="DKRZ",
        description="The data host of eerie.cloud",
        roles=["host"],
        url="https://dkrz.de"
        )
def xarray_to_stac_item(ds):
    #cube
    cube=dict()
    cube['cube:dimensions']=dict()
    cube['cube:variables']=dict()
    for dv in ds.data_vars:
        cube['cube:variables'][dv]=dict(
                type="data",
                dimensions=[*ds[dv].dims],
                unit=ds[dv].attrs.get("units","Not set"),
                description=ds[dv].attrs.get("long_name",dv),
                attrs=ds[dv].attrs
                )

    #default
    title = ds.attrs.get('title', 'No title')
    description = ds.attrs.get('description', 'No description')
    xid = ds.attrs.get('_xpublish_id', 'No id')
    keywords = xid.split('.')
    datetimeattr=datetime.now().isoformat()
    start_datetime = ds.attrs.get('time_min', None).split('.')[0]+'Z' #datetime.now().isoformat())
    end_datetime = ds.attrs.get('time_max', None).split('.')[0]+'Z' #datetime.now().isoformat())
    lonmin=-180
    latmin=-90
    lonmax=180
    latmax=90
    if start_datetime and end_datetime:
        datetimeattr=None
        cube['cube:dimensions']["time"]=dict(
                type="temporal",
                extent=[start_datetime,end_datetime]
                )
    if not "bbox" in ds.attrs and all(a in ds.variables for a in ["lon","lat"]):
        try:
            lonmin=ds["lon"].min().values[()]
            latmin=ds["lat"].min().values[()]
            lonmax=ds["lon"].max().values[()]
            latmax=ds["lat"].max().values[()]
        except:
            pass
        ds.attrs["bbox"]=[
                lonmin,
                latmin,
                lonmax,
                latmax
                ]
    bbox = ds.attrs.get('bbox', [lonmin, latmin, lonmax, latmax])
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [lonmin, latmin],
                [lonmin, latmax],
                [lonmax, latmax],
                [lonmax, latmin],
                [lonmin, latmin]
            ]
        ]
    }
    cdate=ds.attrs.get('creation_date',datetimeattr)
    providers=[copy(PROVIDER_DEFAULT)]
    creator_inst_id=ds.attrs.get('institution_id',None)
    if not creator_inst_id:
        creator_inst_id=ds.attrs.get('centre',None)
    if creator_inst_id:
        creator_inst=ds.attrs.get('institution',None)
        if not creator_inst:
            creator_inst=ds.attrs.get('centreDescription',None)
        creator=copy(PROVIDER_DEFAULT)
        creator["name"]=creator_inst_id
        creator["description"]=creator_inst
        creator["url"]=HOSTURL+xid
        creator["roles"]=["producer"]
        providers.append(creator)
    properties={
            "description": description,
            "title": title,
            "created":cdate,
            "keywords":keywords,
            "providers":providers
        }

    if start_datetime:
        properties["start_datetime"]=start_datetime
        properties["end_datetime"]=end_datetime

    # Create a STAC item
    item = Item(
        id=xid,
        geometry=geometry,
        bbox=bbox,
        datetime=datetimeattr,
        properties=properties,
        stac_extensions=[
            "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
            "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"
            ]
    )

    store_dataset_dict=dict(
            store=HOSTURL,
            dataset=xid+"/zarr"
            )
    var_store_dataset_dict=dict()
    # Add data variables as assets
    for var_name, var_data in ds.data_vars.items():
        item.add_asset(
            var_name,
            Asset(
                href=HOSTURL+xid+"/zarr",
                media_type=MediaType.ZARR,
                roles=["data"],
                title=var_name,
                description=var_data.attrs.get('long_name', ''),
            )
        )
        var_store_dataset_dict[var_name]=copy(store_dataset_dict)

    itemdict=item.to_dict()
    alternatedict=dict(kerchunk={
            "href":HOSTURL+xid+"/kerchunk",
            "alternate:name":"kerchunk",
            "description":"Accessed binary data is as stored on disk via kerchunk"
            })
    for asset in itemdict["assets"].keys():
        itemdict["assets"][asset]["alternate:name"]="dask"
        itemdict["assets"][asset]["alternate:description"]="Accessed binary data is processed on server-side"
        itemdict["assets"][asset]["alternate"]=copy(alternatedict)
    itemdict["links"]=[
            dict(
            rel="DOC",
            href="https://easy.gems.dkrz.de/simulations/EERIE/eerie_data-access_online.html",
            title="Usage of the eerie.cloud"
            ),
            dict(
                rel="DOC",
                href=HOSTURL+xid+"/",
                title="Xarray dataset html representation"
                )
            ]
    #
    #gridlook
    #
    griddict=copy(store_dataset_dict)
    if "icon-esm-er" in xid and "native" in xid:
        if "atmos" in xid or "land" in xid:
            griddict["store"]="https://swift.dkrz.de/v1/dkrz_948e7d4bbfbb445fbff5315fc433e36a/nextGEMS/"
            griddict["dataset"]="grids/ICON_R02B08.zarr"
        elif "ocean" in xid:
            griddict["store"]="https://eerie.cloud.dkrz.de/datasets/"
            griddict["dataset"]="icon-esm-er.eerie-control-1950.v20231106.ocean.native.2d_grid/zarr"

    itemdict["default_var"]=list(var_store_dataset_dict.keys())[0]
    itemdict["name"]=title
    itemdict["levels"]=[
            dict(
                name=xid,
                time=copy(store_dataset_dict),
                grid=griddict,
                datasources=var_store_dataset_dict
                )
            ]
    itemdict["properties"].update(cube)
    for dsatt,dsattval in ds.attrs.items():
        if not dsatt in itemdict["properties"] and not dsatt in itemdict and not "time" in dsatt.lower():
            itemdict["properties"][dsatt]=dsattval

    return itemdict

class Stac(Plugin):
    name: str = "stac"
    mapper_dict: dict = {}

    dataset_router_prefix: str = '/stac'
    dataset_router_tags: Sequence[str] = ['stac']

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags))

        @router.get('/')
        @router.get('')
        async def get_stacitem(ds: xr.Dataset = Depends(deps.dataset)):
#            if not all(a in ds.attrs for a in STACKEYS):
#                raise HTTPException(status_code=404, detail=f"Dataset does not contain all keys needed for a STAC Item")
            item_dict=xarray_to_stac_item(ds)
            return JSONResponse(item_dict)
        return router

