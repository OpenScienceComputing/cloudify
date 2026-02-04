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
import gribscan
import pystac
import fsspec
import requests
from typing import Dict, List, Optional, Union, Any, TypeVar
import math
import socket
from cloudify.utils.mapping import *
from cloudify.utils.defaults import *

PARENT_CATALOG = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/catalogs/stac-catalog-eeriecloud.json"
GRIDLOOK = "https://s3.eu-dkrz-1.dkrz.cloud/bm1344/gridlook/index.html"
GRIDLOOK_HP = "https://gridlook.pages.dev/"
GRIDLOOK = GRIDLOOK_HP
JUPYTERLITE = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/apps/jupyterlite/index.html"
hostname = socket.gethostname()
HOST = f"https://{hostname}"
port = None
if port:
    HOST = f"http://{hostname}:{port}"
HOSTURL = f"{HOST}/datasets"
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

XARRAY_DEF = dict(engine="zarr", chunks="auto")
XARRAY_ZARR = dict(consolidated=True)
XARRAY_KERCHUNK = dict(consolidated=False)
XSO = {"xarray:storage_options": dict(lazy=True)}
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

XarrayDataset = TypeVar("xarray.Dataset")

L_API = False
# HOSTURL="https://s3.eu-dkrz-1.dkrz.cloud"
HOSTURLS = {"AWI": None, "MPI-M": None}
ID_TEMPLATE = "project_id-source_id-experiment_id-version_id-realm-grid_label-level_type-frequency-cell_methods"
ALTERNATE_ASSETS = (
    "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"
)
STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/xarray-assets/v1.0.0/schema.json",
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
]

NEEDED_ATTRS = [
    "title",
    "description",
    "_xpublish_id",
    "time_min",
    "time_max",
    "bbox",
    "creation_date",
    "institution_id",
    "centre",
    "institution",
    "centreDescription",
    "license",
]

INSTITUTE_KEYS = [
    "institution_id",
    "institute_id",
    "institution",
    "institute",
    "centre",
]
SOURCE_KEYS = ["source_id", "model_id", "source", "model"]
EXPERIMENT_KEYS = ["experiment_id", "experiment"]
PROJECT_KEYS = ["project_id", "project", "activity_id", "activity"]

ASSET_TYPES = ["dkrz-disk", "dkrz-cloud", "dkrz-archive", "eerie-cloud"]


def create_stac_collection():
    start = datetime(1850, 1, 1)
    end = datetime(2101, 1, 1)
    col = pystac.Collection(
        id="eerie-cloud-all",
        title="ESM data from DKRZ in Zarr format",
        description=EERIE_DESC,
        stac_extensions=copy(STAC_EXTENSIONS).append(ALTERNATE_ASSETS),
        href=HOST + "/stac-collection-all.json",
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


def make_json_serializable(obj):
    """Recursively convert non-JSON serializable values to serializable formats."""
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, bytes):
        return obj.decode(errors="ignore")  # Convert bytes to string
    else:
        try:
            json.dumps(obj)  # Test if serializable
            return obj  # If it works, return as is
        except (TypeError, OverflowError):
            return str(obj)  # Convert unsupported types to strings


def add_asset_for_ds(item: Item, k: str, ds, item_id: str) -> Item:
    open_kwargs = {}
    open_config = {}
    extra_fields = {}
    if type(ds) == str:
        href = ds
    else:
        href = ds.encoding.get("source", ds.attrs.get("href"))
        volume = ds.nbytes / 1024**2
        extra_fields = {
            "Volume": str(math.ceil(volume / 1024)) + " GB uncompressed",
            "No of data variables": str(len(ds.data_vars)),
        }
        total_chunks = ds.attrs.get("total_no_of_chunks")
        if total_chunks:
            extra_fields["Estimated avergae chunk size [MB]"] = str(
                math.ceil(volume / int(total_chunks))
            )
            extra_fields["Estimated total files (chunks)"] = total_chunks
        open_kwargs = ds.attrs.get("open_kwargs")
        open_config = {
            "xarray:open_kwargs": open_kwargs
            if open_kwargs
            else (copy(XARRAY_DEF) | copy(XARRAY_ZARR)),
            "xarray:storage_options": ds.attrs.get("open_storage_options"),
        }

    if not href:
        print(f"Could not add any asset for item id {item_id}")
        return item
        # raise ValueError("Neither found 'source' in encoding nor 'ref' in attributes")

    if href.startswith("reference"):
        if not open_config.get("xarray:storage_options"):
            open_config["xarray:storage_options"] = copy(XSO)
        if not open_kwargs:
            open_config["xarray:open_kwargs"] = copy(XARRAY_DEF) | copy(XARRAY_KERCHUNK)

    extra_fields.update(open_config)
    access_title = "Zarr-access from Lustre"
    if k == "dkrz-disk":
        newhref = href
        if href.startswith("/"):
            newhref = "file://" + href
        item.add_asset(
            "dkrz-disk",
            Asset(
                href=newhref,
                media_type=MediaType.ZARR,
                roles=["data"],
                title=access_title,
                description="Chunk-based access on Raw Data",
                extra_fields=copy(extra_fields),
            ),
        )
    elif k == "dkrz-cloud":
        access_title = "Zarr-access from Cloud Storage"
        item.add_asset(
            "dkrz-cloud",
            Asset(
                href=href,
                media_type=MediaType.ZARR,
                roles=["data"],
                title=access_title,
                description="Chunk-based access on Raw Data",
                extra_fields=copy(extra_fields),
            ),
        )
    elif k == "eerie-cloud":
        item = add_eerie_cloud_asset(item, href, extra_fields)
        item.add_asset(
            "xarray_view",
            Asset(
                href="/".join(href.split("/")[:-1]) + "/",
                media_type=MediaType.HTML,
                title="Xarray dataset",
                roles=["overview"],
                description="HTML representation of the xarray dataset",
            ),
        )
    elif k == "dkrz-archive":
        access_title = "Zarr-access from Tape Archive"
        item.add_asset(
            "dkrz-tape",
            Asset(
                href=href,
                media_type=MediaType.ZARR,
                roles=["data"],
                title=access_title,
                description="Chunk-based access on raw data",
                extra_fields=copy(extra_fields),
            ),
        )
    else:
        raise ValueError(k + " not implemented")
    return item


def get_item_from_source(
    ds: xr.Dataset,
    item_id: str,
    title: str = None,
    collection_id: str = None,
    exp_license: str = None,
    l_eeriecloud: bool = False,
    main_item_key: str = None,
) -> Item:
    global HOSTURL
    stac_extensions = copy(STAC_EXTENSIONS)
    ds_attrs = get_from_attrs(copy(NEEDED_ATTRS), ds)
    if not title:
        title = ds_attrs.get("title", item_id)
    tmin = ds_attrs.get("time_min")
    if not tmin or "-" not in str(tmin):
        ds_attrs["time_min"], ds_attrs["time_max"] = get_time_min_max(ds)
    if not ds_attrs.get("bbox"):
        ds_attrs["bbox"] = get_bbox(ds)

    stac_href = HOSTURL + "/" + item_id
    if l_eeriecloud:
        stac_href += "/stac"
        stac_extensions.append(ALTERNATE_ASSETS)
    if collection_id:
        stac_href = f"{HOSTURL}/collections/{collection_id}/items/{item_id}"

    providers = get_providers(ds_attrs)
    license = get_spdx_license(exp_license)
    cube = get_cube_extension(ds, ds_attrs["time_min"], ds_attrs["time_max"])

    properties = {
        "title": title,
        "description": get_description(ds, stac_href, main_item_key),
        "created": ds_attrs["creation_date"],
        "keywords": get_keywords(title),
        "providers": providers,
        "license": license,
        "variables": list(cube["cube:variables"].keys()),
        **cube,
    }
    if "crs" in ds.variables:
        hn = ds["crs"].attrs.get("healpix_nside")
        if hn:
            try:
                zoomint = int(math.log2(int(hn)))
                properties["zoom"] = int(zoomint)
            except:
                pass

    for dsatt, dsattval in ds.attrs.items():
        if not properties.get(dsatt) and not "time" in dsatt.lower():
            properties[dsatt] = dsattval

    datetimeattr = datetime.now()
    sdt = properties.get("start_datetime")
    if ds_attrs.get("time_min") != None:
        datetimeattr = None
        properties["start_datetime"] = ds_attrs["time_min"]
        properties["end_datetime"] = ds_attrs["time_max"]
    elif sdt:
        if "-" not in str(sdt):
            properties["start_datetime"] = None
            properties["end_datetime"] = None
        else:
            datetimeattr = None

    geometry = get_geometry(ds_attrs["bbox"])

    # Create a STAC item
    item = Item(
        id=item_id,
        geometry=geometry,
        bbox=ds_attrs["bbox"],
        # bbox=get_bbox(ds),
        datetime=datetimeattr,
        properties=properties,
        stac_extensions=stac_extensions,
    )

    return item


def xarray_zarr_datasets_to_stac_item(
    dset_dict_inp: dict,
    item_id: str = None,
    collection_id: str = None,
    exp_license: str = None,
    title: str = None,
) -> dict:
    global ASSET_TYPES
    dset_dict = {a: b for a, b in dset_dict_inp.items() if b is not None}

    if not dset_dict:
        raise ValueError("Your input does not have any datasets")

    main_item_key = None
    item_ds = None
    for a in ASSET_TYPES:
        if a in dset_dict:
            if isinstance(dset_dict[a], xr.Dataset):
                item_ds = dset_dict[a]
                main_item_key = None
                break

    if not item_ds:
        raise ValueError(
            f"asset types {','.join(list(dset_dict.keys()))} not in valid asset types {','.join(ASSET_TYPES)}"
        )

    l_eerie = "eerie-cloud" in dset_dict
    dscloud = dset_dict.get("eerie-cloud")
    if not isinstance(dscloud, xr.Dataset):
        dscloud = dset_dict.get("dkrz-cloud")
    elif dscloud.equals(item_ds) and not item_id:
        item_id = dscloud.attrs["_xpublish_id"]
    if not isinstance(dscloud, xr.Dataset):
        dscloud = None

    item = get_item_from_source(
        item_ds,
        item_id,
        title,
        collection_id,
        exp_license,
        main_item_key=main_item_key,
        l_eeriecloud=l_eerie,
    )

    for k, ds in dset_dict.items():
        item = add_asset_for_ds(item, k, ds, item_id)

    if dscloud:
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
        gridlook_href = GRIDLOOK + "#"
        if l_eerie:
            gridlook_href += defaults["EERIE_CLOUD_URL"] + "/" + item.id + "/stac"
        else:
            gridlook_href += dscloud.encoding["source"]
        item.add_asset(
            "gridlook",
            Asset(
                href=gridlook_href,
                media_type=MediaType.HTML,
                title="Visualization with gridlook",
                roles=["Visualization"],
                description="Visualization with gridlook",
            ),
        )

    itemdict = item.to_dict()
    itemdict = make_json_serializable(itemdict)

    itemdict = get_and_set_zoom(itemdict)
    itemdict["providers"] = [defaults["PROVIDER_DKRZ"]]
    itemdict["properties"]["asset_types"] = list(dset_dict.keys())
    for delitem in ["summary", "history"]:
        itemdict["properties"].pop(delitem, None)
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
    itemdict = get_gridlook(itemdict, item_ds)
    itemdict = make_json_serializable(itemdict)

    return itemdict


def enrich_ds_with_apiattrs_from_conf(
    item_ds: XarrayDataset,
    local_conf: Dict[str, str],
    item_title: str,
    full_metadata_from_intake: bool = False,
    overwrite: bool = False,
) -> XarrayDataset:
    """
    Prepare Xarray dataset for STAC item creation by setting required attributes.

    Args:
        item_ds: Xarray dataset to prepare
        local_conf: Configuration dictionary containing metadata
        item_title: Title of the STAC item

    Returns:
        Prepared Xarray dataset with required attributes set
    """
    if full_metadata_from_intake:
        item_ds.attrs.update(local_conf.get("from_intake"))
    stac_collection_id = copy(defaults["STAC_COLLECTION_ID_TEMPLATE"])
    for template_element_raw in defaults["STAC_COLLECTION_ID_TEMPLATE"].split("-") + [
        "frequency"
    ]:
        template_element = template_element_raw.replace("<", "").replace(">", "")
        if not item_ds.attrs.get(template_element) or overwrite:
            if local_conf.get(template_element):
                item_ds.attrs[template_element] = local_conf[template_element]
    item_ds = set_grid_label(item_title, item_ds)
    item_ds = set_realm(item_title, item_ds)
    return set_frequency(item_title, item_ds)


def get_spdx_license(exp_license: str) -> str:
    if exp_license:
        URL_LIST_OF_LICENSES = "https://raw.githubusercontent.com/spdx/license-list-data/refs/heads/main/json/licenses.json"
        lol = json.load(fsspec.open(URL_LIST_OF_LICENSES).open())["licenses"]
        for license_dict in lol:
            if (
                exp_license == license_dict["licenseId"]
                or exp_license in license_dict["name"]
                or license_dict["name"] in exp_license
            ):
                return license_dict["licenseId"]
    return "other"


def get_bbox(
    ds: xr.Dataset,
    lonmin: float = -180.0,
    latmin: float = -90.0,
    lonmax: float = 180.0,
    latmax: float = 90.0,
) -> list:
    return [lonmin, latmin, lonmax, latmax]
    if all(a in ds.variables for a in ["lon", "lat"]) and all((ds[a].chunks) for a in ds.data_vars):
        ds_withoutcoords = ds.reset_coords()
        try:
            lonmin = ds_withoutcoords["lon"].min().values[()]
            latmin = ds_withoutcoords["lat"].min().values[()]
            lonmax = ds_withoutcoords["lon"].max().values[()]
            latmax = ds_withoutcoords["lat"].max().values[()]
        except:
            pass

    return [lonmin, latmin, lonmax, latmax]


def get_cube_extension(ds: xr.Dataset, time_min: str, time_max: str) -> dict:
    cube = dict()
    cube["cube:dimensions"] = dict()
    cube["cube:variables"] = dict()
    for dv in ds.data_vars:
        cube["cube:variables"][dv] = dict(
            type="data",
            dimensions=[*ds[dv].dims],
            unit=ds[dv].attrs.get("units", "Not set"),
            description=ds[dv].attrs.get("long_name", dv),
            # attrs=ds[dv].attrs
        )
    if time_min and time_max:
        cube["cube:dimensions"]["time"] = dict(
            type="temporal", extent=[time_min, time_max]
        )
    return cube


def get_from_attrs(needed_attrs: list, ds: xr.Dataset) -> dict:
    datetimeattr = datetime.now()  # .isoformat()
    from_attrs = dict()
    for key in needed_attrs:
        ds_attr = ds.attrs.get(key)
        if ds_attr:
            from_attrs[key] = str(ds_attr)
            if key == "time_min" or key == "time_max":
                from_attrs[key] = from_attrs[key].split(".")[0] + "Z"
        if key == "creation_date" and not from_attrs.get(key):
            from_attrs[key] = datetimeattr

    return from_attrs


def get_geometry(bbox: list) -> dict:
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [bbox[0], bbox[1]],
                [bbox[0], bbox[3]],
                [bbox[2], bbox[3]],
                [bbox[2], bbox[1]],
                [bbox[0], bbox[1]],
            ]
        ],
    }


def get_time_min_max(ds: xr.Dataset) -> (str, str):
    time_min = time_max = None
    if "time" in ds.variables:
        time_min = str(ds["time"].min().values[()]).split(".")[0] + "Z"
        time_max = str(ds["time"].max().values[()]).split(".")[0] + "Z"
    return time_min, time_max


def get_providers(ds_attrs: dict) -> list:
    providers = [copy(defaults["PROVIDER_DKRZ"])]
    creator_inst_id = ds_attrs.get("institution_id", ds_attrs.get("centre"))
    if creator_inst_id:
        creator_inst = ds_attrs.get(
            "institution", ds_attrs.get("centreDescription", "N/A")
        )
        creator = copy(defaults["PROVIDER_DKRZ"])
        creator["name"] = creator_inst_id
        creator["description"] = creator_inst
        creator["url"] = HOSTURLS.get(creator_inst_id, "N/A")
        creator["roles"] = ["producer"]
        providers.append(creator)
    return providers


def get_keywords(keywordstr: str) -> list:
    split1 = keywordstr.split("-")
    split2 = [a.split("_") for a in split1]
    return [element for sublist in split2 for element in sublist]


def get_description(ds: xr.Dataset, href: str = None, main_item_key: str = None) -> str:
    source = next(
        (
            ds.attrs.get(default)
            for default in SOURCE_KEYS
            if ds.attrs.get(default) is not None
        ),
        "not Set",
    )
    exp = next(
        (
            ds.attrs.get(default)
            for default in EXPERIMENT_KEYS
            if ds.attrs.get(default) is not None
        ),
        "not Set",
    )
    project = next(
        (
            ds.attrs.get(default)
            for default in PROJECT_KEYS
            if ds.attrs.get(default) is not None
        ),
        "not Set",
    )
    institute = next(
        (
            ds.attrs.get(default)
            for default in INSTITUTE_KEYS
            if ds.attrs.get(default) is not None
        ),
        "not Set",
    )
    description = (
        "\n\nSimulation data from project '"
        + project
        + "' produced by Earth System Model '"
        + source
        + "' and run by institution '"
        + institute
        + "' for the experiment '"
        + exp
        + "'"
    )
    if href:
        snippet = defaults["ITEM_SNIPPET"].replace(
            "REPLACE_ITEMURI",
            #'"'+defaults["STAC_API_URL_EXT"]+"/collections/"+stac_collection_id_lower+"/items/"+item_id+'"'
            href,
        )
        if main_item_key:
            snippet = snippet.replace("dkrz-disk", main_item_key)
        description += snippet

    if ds.attrs.get("title"):
        description += "\n\n # " + ds.attrs.get("title")
    for descatt in ["description", "summary"]:
        if ds.attrs.get(descatt):
            description += "\n\n" + ds.attrs.get(descatt)
    return description


def refine_for_eerie(item_id: str, griddict: dict) -> dict:
    if any(b in item_id for b in ["icon-esm-er","icon-epoc"]) and "native" in item_id:
        griddict[
            "store"
        ] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
        if "atmos" in item_id or "land" in item_id:
            griddict["dataset"] = "icon_grid_0033_R02B08_G.zarr"
        elif "ocean" in item_id:
            griddict["dataset"] = "icon_grid_0016_R02B09_O.zarr"
    elif all(a in item_id for a in ["ifs-fesom", "native", "ocean"]):
        griddict["dataset"]=griddict["dataset"].replace("/kerchunk","/zarr")
    elif "gr025" in item_id:
        if not "ifs" in item_id :
            griddict["store"] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
            griddict["dataset"] = "gr025.zarr"
        else:
            griddict["dataset"] = griddict["dataset"].replace("/kerchunk","/zarr")
    elif "era5" in item_id:
        griddict[
            "store"
        ] = "https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/grids/"
        griddict["dataset"] = "era5.zarr"

    return griddict


def get_gridlook(itemdict: dict, ds: xr.Dataset, alternative_uri: str = None) -> dict:
    global L_API
    if not L_API:
        item_id = itemdict["id"]
        if alternative_uri:
            store_dataset_dict = dict(
                store="/".join(alternative_uri.split("/")[0:-1]),
                dataset=alternative_uri.split("/")[-1],
            )
        else:
            if any(ds[a].attrs.get("original_compressor") for a in ds.data_vars):
                store_dataset_dict = dict(
                    store=defaults["EERIE_CLOUD_URL"] + "/",
                    dataset=item_id + "/zarr",
                )                
            else:
                store_dataset_dict = dict(
                    store=defaults["EERIE_CLOUD_URL"] + "/",
                    dataset=item_id + "/kerchunk",
                )
        var_store_dataset_dict = dict()
        # Add data variables as assets
        #    for var_name, var_data in ds.data_vars.items():
        for var_name, var_data in ds.variables.items():
            var_store_dataset_dict[var_name] = copy(store_dataset_dict)

        best_choice = next(
            (
                a
                for a in list(var_store_dataset_dict.keys())
                if a in ["uas", "si", "tas", "10ws", "ws"]
            ),
            None,
        )
        if not best_choice:
            best_choice = next(
                (
                    a
                    for a in list(var_store_dataset_dict.keys())
                    if not any(b in a for b in ["lat", "lon"])
                ),
                None,
            )
        if not best_choice:
            print("Oh no best choice")
            best_choice == list(var_store_dataset_dict.keys())[0]
        itemdict["default_var"] = best_choice
        itemdict["name"] = itemdict["properties"]["title"]

        griddict = copy(store_dataset_dict)
        if not alternative_uri:
            griddict = refine_for_eerie(item_id, griddict)

        itemdict["levels"] = [
            dict(
                name=item_id,
                time=dict(
                    store=defaults["EERIE_CLOUD_URL"] + "/",
                    dataset=item_id + "/zarr",
                    ),
                grid=griddict,
                datasources=var_store_dataset_dict,
            )
        ]
    return itemdict


def add_eerie_cloud_asset(item: Item, href: str, extra_fields: dict) -> Item:
    href_kerchunk = "/".join(href.split("/")[:-1]) + "/kerchunk"
    href_zarr = "/".join(href.split("/")[:-1]) + "/zarr"
    extra_fields["alternate"] = copy(ALTERNATE_KERCHUNK)
    extra_fields["alternate"]["processed"]["href"] = href_zarr
    extra_fields["alternate"]["processed"][
        "name"
    ] = "Rechunked and uniformly compressed data"
    item.add_asset(
        "eerie-cloud",
        Asset(
            href=href_kerchunk,
            media_type=MediaType.ZARR,
            roles=["data"],
            title="Zarr-access through Eerie Cloud",
            description="Chunk-based access on raw-encoded data",
            extra_fields=extra_fields,
        ),
    )
    return item


def add_links(itemdict: dict, l_eeriecloud: bool) -> dict:
    if l_eeriecloud:
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
                href="/".join(
                    defaults["EERIE_CLOUD_URL"].split("/")[:-1]
                    + ["stac-collection-all.json"]
                ),
                type="application/json",
            )
        )
    return itemdict


def make_json_serializable(obj):
    """Recursively convert non-JSON serializable values to serializable formats."""
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, bytes):
        return obj.decode(errors="ignore")  # Convert bytes to string
    else:
        try:
            json.dumps(obj)  # Test if serializable
            return obj  # If it works, return as is
        except (TypeError, OverflowError):
            return str(obj)  # Convert unsupported types to strings


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
            dslist = eval(requests.get(HOSTURL).text)
            for item in dslist:
                coldict["links"].append(
                    dict(
                        rel="child",
                        href=HOSTURL + "/" + item + "/stac",
                        title=item,
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
                dssource = ds.encoding.pop("source", None)
                ds.encoding["source"] = "/".join(
                    [HOSTURL, ds.attrs["_xpublish_id"], "zarr"]
                )

                item_dict = xarray_zarr_datasets_to_stac_item(
                    {"dkrz-disk": dssource, "eerie-cloud": ds}
                )
                item_dict["properties"]["title"]=ds.attrs["_xpublish_id"]
                ds.encoding["source"] = dssource
                resp = JSONResponse(item_dict)
                # except:
                #    raise HTTPException(status_code=404, detail="Could not create a STAC Item")
                cache.put(cache_key, resp, 99999)
            return resp

        return router
