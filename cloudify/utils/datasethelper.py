import numcodecs
import gribscan
import numpy as np
import intake
import itertools
import fsspec
import xarray as xr
from datetime import datetime
from copy import deepcopy as copy
import fastapi

STORAGE_OPTIONS = dict(
    #        remote_protocol="file",
    lazy=True,
    cache_size=0
    #        skip_instance_cache=True,
    #        listings_expiry_time=0
)

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
DEFAULT_COORDS = [
    "cell_sea_land_mask",
    "lat",
    "lon",
    "coast",
    "vertices_latitude",
    "vertices_longitude",
    "latitude",
    "longitude",
]


def set_custom_header(response: fastapi.Response) -> None:
    response.headers["Cache-control"] = "max-age=3600"
    response.headers["X-EERIE-Request-Id"] = "True"
    response.headers["Last-Modified"] = datetime.today().strftime(
        "%a, %d %b %Y 00:00:00 GMT"
    )


def split_ds(ds: xr.Dataset) -> list:
    fv = None
    for v in ds.data_vars:
        if "time" in ds[v].dims:
            fv = v
            break
    if not fv:
        return [ds]
    dimno = 0
    for dimno, dim in enumerate(ds[v].dims):
        if dim == "time":
            break
    first = ds[fv].chunks[dimno][0]
    subdss = []
    sumchunks = 0
    lastsum = 0
    for i in range(len(ds[fv].chunks[0])):
        f = ds[fv].chunks[dimno][i]
        sumchunks += f
        if f != first:
            dss = ds.isel(time=slice(lastsum, sumchunks))
            subdss.append(dss)
            lastsum = sumchunks
    if not subdss:
        return [ds]
    if len(subdss) == 1:
        return [ds]
    return subdss


def get_dask_options(desc: dict, l_dask: bool = True) -> dict:
    options = dict(storage_options=desc["args"].get("storage_options", {}))
    if l_dask:
        options["storage_options"].update(STORAGE_OPTIONS)
    else:
        options["chunks"] = None
    return options


def chunk_and_prepare_fesom(ds: xr.Dataset) -> xr.Dataset:
    chunk_dict = dict(nod2=1000000, nz1_upper=6, nz1=6, nz_upper=6, nz=6)  # , time=4)
    keylist = [k for k in chunk_dict.keys() if k not in ds.dims]
    if "heightAboveGround" in ds.variables:
        ds = ds.drop("heightAboveGround")
    for k in keylist:
        del chunk_dict[k]
    return ds.chunk(chunk_dict)


def set_or_delete_coords(ds: xr.Dataset, dsid: str) -> xr.Dataset:
    global DEFAULT_COORDS
    to_coords = [a for a in ds.data_vars if a in DEFAULT_COORDS]
    more_coords = [
        a for a in ds.data_vars if ("bnds" in a or "bounds" in a) and a not in to_coords
    ]
    to_coords += more_coords
    for a in to_coords:
        if "time" in ds[a].dims and a != "time_bnds" and a != "time_bounds":
            ds[a] = ds[a].isel(time=0)
    if to_coords:
        ds = ds.set_coords(to_coords)
    if "hadgem" in dsid:
        other_coords = [a for a in ds.coords if a not in to_coords]
        other_coords += more_coords
        for oc in other_coords:
            if not "time" in oc:
                print(oc)
                del ds[oc]
    return ds


def gribscan_to_float(ds: xr.Dataset) -> xr.Dataset:
    for dv in ds.data_vars:
        compressor = ds[dv].encoding.get("compressor")
        if compressor:
            if (
                type(compressor) == gribscan.rawgribcodec.RawGribCodec
                and ds[dv].encoding["dtype"] == np.float64
            ):
                ds[dv] = ds[dv].astype("float32")
    return ds


def lossy_compress_chunk(partds):
    import numcodecs

    rounding = numcodecs.BitRound(keepbits=12)
    return rounding.decode(rounding.encode(partds))


def apply_lossy_compression(
    mapper_dict: dict, dsid: str, ds: xr.Dataset, L_DASK: bool = True
) -> (dict, xr.Dataset):
    print("lossy")
    if L_DASK:
        ds = xr.apply_ufunc(
            lossy_compress_chunk, ds, dask="parallelized", keep_attrs="drop_conflicts"
        )
    else:
        mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsid, ds)
        for var in ds.data_vars:
            ds[var].encoding["filters"] = numcodecs.BitRound(keepbits=12)
    return mapper_dict, ds


def set_compression(ds):
    for var in ds.data_vars:
        #                ds[var].encoding["compressor"]=None
        ds[var].encoding = {
            "compressor": numcodecs.Blosc(
                cname="lz4", clevel=5, shuffle=2
            )  # , blocksize=0),
        }
    return ds


def find_data_sources(catalog, name=None):
    newname = ".".join([a for a in [name, catalog.name] if a])
    data_sources = []

    for key, entry in catalog.items():
        if isinstance(entry, intake.source.csv.CSVSource):
            continue
        if isinstance(entry, intake.catalog.Catalog):
            if newname == "main":
                newname = None
                # If the entry is a subcatalog, recursively search it
            data_sources.extend(find_data_sources(entry, newname))
        elif isinstance(entry, intake.source.base.DataSource):
            data_sources.append(newname + "." + key)

    return data_sources


def get_combination_list(ups: dict) -> list:
    uplists = [up["allowed"] for up in ups]
    combinations = list(itertools.product(*uplists))
    return [{ups[i]["name"]: comb[i] for i in range(len(ups))} for comb in combinations]


def reset_encoding_get_mapper(mapper_dict, dsid, ds, desc=None):
    sp = None
    if "source" in ds.encoding:
        sp = ds.encoding["source"]
    elif desc:
        updesc = desc["args"]["urlpath"]
        if type(updesc) == str or (type(updesc) == list and len(updesc) == 1):
            if type(updesc) == list:
                updesc = updesc[0]
            sp = updesc
    ds = ds.reset_encoding()
    if sp:
        use_options = copy(STORAGE_OPTIONS)
        if desc:
            desc_so = desc["args"].get("storage_options")
            if desc_so:
                use_options.update(desc_so)
        if not use_options.get("remote_protocol") and sp.startswith("reference"):
            use_options["remote_protocol"] = "file"
        print(use_options)
        mapper_dict[sp] = fsspec.get_mapper(sp, **use_options)
        ds.encoding["source"] = sp
    return mapper_dict, ds


frequency_mapping = {
    "10m": "10m",
    "15m": "15m",
    "3h": "3hr",
    "6h": "6hr",
    "1h": "1hr",
    "4h": "4hr",
    "24h": "daily",
    "1d": "daily",
    "day": "daily",
    "daily": "daily",
    "mon": "monthly",
    "1m": "monthly",
    "1y": "yearly",
    "year": "yearly",
    "grid": "fx",
}

# Function to determine frequency
def set_frequency(inp, ds):
    for keyword, frequency in frequency_mapping.items():
        if keyword in inp:
            ds.attrs["frequency"] = frequency
            break
    else:
        ds.attrs["frequency"] = "unknown"  # Default value if no match is found
    return ds


def adapt_for_zarr_plugin_and_stac(dsid, ds):
    title = ds.attrs.get("title", "default")
    if title in ["default", "ICON simulation"]:
        ds.attrs["title"] = dsid
    desc = ds.attrs.get("description")
    if not desc:
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

        ds.attrs["description"] = (
            "Simulation data from project '"
            + project
            + "' produced by Earth System Model '"
            + source
            + "' and run by institution '"
            + institute
            + "' for the experiment '"
            + exp
            + "'"
        )
    if "time" in ds.variables:
        if ds["time"].attrs.get("_FillValue"):
            del ds["time"].attrs["_FillValue"]
        ds["time"].encoding["dtype"] = "float64"
        ds["time"].encoding["compressor"] = None
        ds.attrs["time_min"] = str(ds["time"].values[0])
        ds.attrs["time_max"] = str(ds["time"].values[-1])
        for att in ["units", "calendar"]:
            if ds["time"].attrs.get(att) and not ds["time"].encoding.get(att):
                ds["time"].encoding[att] = ds["time"].attrs[att]
                del ds["time"].attrs[att]
    else:  # for freva
        if not ds.attrs.get("time_min"):
            ds.attrs["time_min"] = None
        if not ds.attrs.get("time_max"):
            ds.attrs["time_max"] = None
    ds = set_frequency(dsid.split(".")[-1].lower(), ds)
    ds.attrs["creation_date"] = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    return ds
