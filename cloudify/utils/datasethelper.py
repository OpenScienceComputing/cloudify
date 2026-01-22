import numcodecs
import gribscan
import numpy as np
import math
import intake
import itertools
import xarray as xr
from datetime import datetime
from copy import deepcopy as copy
import fastapi
import numcodecs
import fsspec
from fsspec.core import url_to_fs
from fsspec.mapping import FSMap
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
import asyncio
import numcodecs
import zarr
from dask.array import Array as Daarray
from pathlib import Path
import json

rounding = numcodecs.BitRound(keepbits=10)

STORAGE_OPTIONS = dict(
    #        remote_protocol="file",
    lazy=True,
    #cache_size=128#0
    #        skip_instance_cache=True,
    #        listings_expiry_time=0
)

INSTITUTE_KEYS = [
    "institution_id",
    "institute_id",
    "institution",
    "institute",
    "centre"
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
    "longitude"
]

class AsyncFSMap(FSMap):
    """
    Async version of FSMap: dict-like interface to a filesystem.
    Works only with AsyncFileSystem backends.
    """

    def __init__(self, root, fs, check=False, create=False, missing_exceptions=(FileNotFoundError,),**kwargs):
        if not fs.async_impl:
            print("not async")
            fs = AsyncFileSystemWrapper(fs, asynchronous=False)
        super().__init__(
            root, fs, check=False, create=False, missing_exceptions=(FileNotFoundError,)
        )
        self.use_options=kwargs

    async def asyncitem(self, key):
        """Retrieve data asynchronously"""
        k = self._key_to_str(key)
        #try:
        result = await self.fs._cat(k)
        #except self.missing_exceptions as exc:
        #    raise KeyError(key) from exc
        return result

def async_get_mapper(
    url="",
    check=False,
    create=False,
    missing_exceptions=None,
    alternate_root=None,
    **kwargs,
):
    fs, urlpath = url_to_fs(url, **kwargs)
    root = alternate_root if alternate_root is not None else urlpath    
    return AsyncFSMap(root, fs, check, create, missing_exceptions=missing_exceptions, **kwargs)    
    #return FSMap(root, fs, check, create, missing_exceptions=missing_exceptions)

def is_nan(x):
    return (
        x is None or
        (isinstance(x, float) and math.isnan(x)) or
        x is np.nan
    )

def sanitize_for_json(obj):
    """Recursively convert zarr/numcodecs objects into JSON-safe dicts."""
    # Handle codec or filter objects
    if isinstance(obj, numcodecs.abc.Codec):
        try:
            compr_config=compressor.get_config()
            return compr_config
        except:
            pass
        d = {"id": obj.codec_id}
        # Add common attributes if they exist
        for attr in ("cname", "clevel", "shuffle", "blocksize"):
            if hasattr(obj, attr):
                d[attr] = getattr(obj, attr)
        return d

    # Handle numpy dtypes and similar
    if isinstance(obj, np.dtype):
        return str(obj)

    # Recursively sanitize dicts and lists
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if k == "pr" or k == "pl":
                continue
            cv = sanitize_for_json(v)
            if not is_nan(cv):
                cleaned[k] = cv
            else:
                cleaned[k] = None
        return cleaned        
    if isinstance(obj, (list, tuple)):
        cleaned_list = []
        for v in obj:
            cv = sanitize_for_json(v)
            if not is_nan(cv):
                cleaned_list.append(cv)
        return cleaned_list    
    return obj

async def extract_zarr_json_tree(store: zarr.storage.MemoryStore) -> dict:
    """
    Collect all zarr.json files into a tree dict.
    """
    tree = {}

    async for key in store.list():        
        if key.endswith("zarr.json"):
            path = key[:-10].strip("/")
            node = tree
            if path:
                for part in path.split("/"):
                    node = node.setdefault(part, {})
            data = await store.get(key)
            data = data.to_bytes()   # convert to bytes
            node["zarr.json"] = json.loads(data.decode())

    return tree

async def consolidate_zmetadatas_for_tree(zmetadatas: dict[str, dict]) -> dict:
    """
    Build a Zarr v3 hierarchy from array metadata and return the zarr.json tree.

    Each input entry is assumed to describe a single array.
    """
    store = zarr.storage.MemoryStore()

    root = zarr.group(store=store, overwrite=True)

    for root_name, meta in zmetadatas.items():
        if "metadata" not in meta:
            raise ValueError(f"{root_name} is not a valid metadata dict")

        # Extract array metadata (former .zarray)
        array_meta = None
        attrs = {}

        for key, value in meta["metadata"].items():
            if key.endswith(".zarray"):
                array_meta = value
            elif key.endswith(".zattrs"):
                attrs = value

        if array_meta is None:
            raise ValueError(f"{root_name} has no array metadata")

        path = Path(root_name)

        # Create intermediate groups
        grp = root
        for part in path.parts[:-1]:
            grp = grp.require_group(part)

        # Create array (metadata written automatically)
        arr = grp.create_array(
            path.parts[-1],
            shape=tuple(array_meta["shape"]),
            dtype=array_meta["dtype"],
            chunks=array_meta.get("chunks"),
            fill_value=array_meta.get("fill_value"),
            overwrite=True,
        )

        # Assign attributes
        arr.attrs.update(attrs)

    # Extract zarr.json tree
    return await extract_zarr_json_tree(store)

def consolidate_zmetadatas_for_tree_v2(zmetadatas: dict[str, dict]) -> dict:
    """
    Consolidate multiple single-group zmetadata dicts into one higher-level hierarchy.
    
    Assumes each zmetadata contains only one group (no nested subgroups inside).
    Creates intermediate subgroups for all keys based on '/'.
    """
    store = zarr.storage.MemoryStore()

    for root_name, zmeta in zmetadatas.items():
        if "metadata" not in zmeta:
            raise ValueError(f"{root_name} is not a valid .zmetadata dict")

        # Collect variable parent paths (parent of .zarray/.zattrs)
        variable_parents = set()
        for key in zmeta["metadata"]:
            if key.endswith((".zarray", ".zattrs")):
                full_path = Path(root_name) / key
                variable_parents.add(str(full_path.parent).replace("\\", "/"))

        for key, value in zmeta["metadata"].items():
            full_key = Path(root_name) / key
            full_key_str = str(full_key).replace("\\", "/")
            value = sanitize_for_json(value)

            # Write array metadata or other entries
            if full_key_str.endswith(".zarray"):
                if not isinstance(value.get("dtype"), str):
                    encoded = encode_array_metadata(value)
                else:
                    encoded = json.dumps(value)
                store[full_key_str] = encoded.encode()
            else:
                store[full_key_str] = json.dumps(value).encode()

            # Create .zgroup for all parent paths except variable parent
            for dotz in [".zgroup",".zattrs"]:
                parent = full_key.parent                
                while str(parent) != ".":
                    gkey = str(parent / dotz).replace("\\", "/")
                    if gkey not in store and str(parent) not in variable_parents:
                        store[gkey] = json.dumps({}).encode()                        
                        if dotz == ".zgroup":
                            store[gkey] = json.dumps({"zarr_format": 2}).encode()
                    parent = parent.parent
                    
    # Root group
    store[".zgroup"] = json.dumps({"zarr_format": 2}).encode()

    # Consolidate
    zarr.consolidate_metadata(store)
    return json.loads(store[".zmetadata"].decode())
    
def set_custom_header(response: fastapi.Response) -> None:
    response.headers["Cache-control"] = "max-age=3600"
    response.headers["X-EERIE-Request-Id"] = "True"
    response.headers["Last-Modified"] = datetime.today().strftime(
        "%a, %d %b %Y 00:00:00 GMT"
    )
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST,GET,HEAD"
    response.headers["Access-Control-Allow-Headers"] = "*"


def split_ds(ds: xr.Dataset) -> list[xr.Dataset]:
    """
    Split an xarray dataset into sub-datasets based on chunk sizes.
    
    This function splits the dataset when chunk sizes change along the time dimension.
    It's particularly useful for handling datasets with varying chunk sizes.
    
    Args:
        ds (xr.Dataset): The input xarray dataset to split
        
    Returns:
        list[xr.Dataset]: List of sub-datasets split at chunk boundaries
    """
    fv = None
    # Find the first variable with a time dimension
    for v in ds.data_vars.keys():
        if "time" in ds[v].dims:
            fv = v
            break
    
    if not fv:
        return [ds]
    
    # Find the time dimension index
    dimno = 0
    for dimno, dim in enumerate(ds[v].dims):
        if dim == "time":
            break
    
    # Get the first chunk size
    first = ds[fv].chunks[dimno][0]
    subdss = []
    sumchunks = 0
    lastsum = 0
    
    # Split dataset at chunk boundaries
    for i in range(len(ds[fv].chunks[0])):
        f = ds[fv].chunks[dimno][i]
        sumchunks += f
        if f != first:
            dss = ds.isel(time=slice(lastsum, sumchunks))
            subdss.append(dss)
            lastsum = sumchunks
    
    # Return original dataset if no splits were made
    if not subdss:
        return [ds]
    
    # Return original dataset if only one split was made
    if len(subdss) == 1:
        return [ds]
    
    return subdss

def open_zarr_and_mapper(uri, storage_options=None,**kwargs):    
    use_options = copy(STORAGE_OPTIONS)
    if storage_options:
        use_options.update(storage_options)
    if uri.startswith("reference"):
        if not use_options.get("remote_protocol"):
            use_options["remote_protocol"] = "file"
        # this is set by zarr v3 in the xarray: use_options["asynchronous"]=True
        # which is why we need it for the mapper and the following:            
        elif use_options.get("remote_protocol").startswith("http"):
            if use_options.get("remote_options"):
                use_options["remote_options"]["asynchronous"]=True
                use_options["remote_options"]["loop"]=None
            else:
                use_options["remote_options"]=dict(asynchronous=True,loop=None)
            
    use_options["asynchronous"]=True
    use_options["loop"]=None
    mapper = fsspec.get_mapper(uri, **use_options)
    ds = xr.open_dataset(
            mapper,
            #uri,
            #storage_options=use_options,
            engine="zarr",
            create_default_indexes=False,
            **kwargs
            )
    if "time" in ds:
        if isinstance(ds["time"].data, Daarray):
            ds["time"] = ds["time"].data.compute()   
            
    use_options["asynchronous"]=False
    lp = asyncio.get_running_loop()
    use_options["loop"]=lp
    if uri.startswith("reference"):    
        if not use_options.get("remote_options"):
            use_options["remote_options"]=dict(loop=lp)
        else:
            use_options["remote_options"]["loop"]=lp
        use_options["remote_options"]["asynchronous"]=False        
    
    asyncmapper = async_get_mapper(uri, **use_options)                
    return ds,asyncmapper

def chunk_and_prepare_fesom(ds: xr.Dataset) -> xr.Dataset:
    """
    Prepare FESOM dataset with appropriate chunking.
    
    This function sets better chunk sizes for FESOM datasets and removes variables that prevents serving.
    
    Args:
        ds (xr.Dataset): Input FESOM dataset
        
    Returns:
        xr.Dataset: Chunked and prepared dataset
    """
    chunk_dict = dict(nod2=1000000, nz1_upper=6, nz1=6, nz_upper=6, nz=6)
    keylist = [k for k in chunk_dict.keys() if k not in ds.dims]
    
    # Drop heightAboveGround variable if present
    if "heightAboveGround" in ds.variables:
        ds = ds.drop("heightAboveGround")
    
    # Remove chunk dimensions not present in dataset
    for k in keylist:
        del chunk_dict[k]
    
    return ds.chunk(chunk_dict)


def set_or_delete_coords(ds: xr.Dataset, dsid: str) -> xr.Dataset:
    """
    Set or delete coordinates in a dataset based on dataset type.

    This function manages coordinates in the dataset by:
    1. Setting default coordinates from DEFAULT_COORDS
    2. Adding boundary/bounds coordinates
    3. Selecting first time step for time-dependent coordinates
    4. Removing unnecessary coordinates for HadGEM datasets

    Args:
        ds (xr.Dataset): Input dataset to modify
        dsid (str): Dataset ID used to determine special handling

    Returns:
        xr.Dataset: Modified dataset with appropriate coordinates

    Raises:
        ValueError: If input dataset is invalid
    """
    if not isinstance(ds, xr.Dataset):
        raise ValueError("Input must be an xarray Dataset")
    
    to_coords = [a for a in ds.data_vars if a in DEFAULT_COORDS]
    more_coords = [
        a for a in ds.data_vars if ("bnds" in a or "bounds" in a) and a not in to_coords
    ]
    to_coords += more_coords
    
    # Select first time step for time-dependent coordinates
    for a in to_coords:
        if "time" in ds[a].dims and a != "time_bnds" and a != "time_bounds":
            ds[a] = ds[a].isel(time=0)
    
    # Set coordinates
    if to_coords:
        ds = ds.set_coords(to_coords)
    
    # Special handling for HadGEM datasets
    if "hadgem" in dsid:
        other_coords = [a for a in ds.coords if a not in to_coords]
        other_coords += more_coords
        for oc in other_coords:
            if not "time" in oc:
                print(f"Removing coordinate: {oc}")
                del ds[oc]
    
    return ds


def gribscan_to_float(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert GRIB dataset variables from float64 to float32.

    This function converts variables compressed with RawGribCodec
    from float64 to float32 to reduce memory usage while maintaining
    sufficient precision.

    Args:
        ds (xr.Dataset): Input GRIB dataset with float64 variables

    Returns:
        xr.Dataset: Dataset with converted float32 variables

    Raises:
        ValueError: If input dataset is invalid
    """
    if not isinstance(ds, xr.Dataset):
        raise ValueError("Input must be an xarray Dataset")
    
    for dv in ds.data_vars:
        compressor = ds[dv].encoding.get("compressor")
        if compressor and isinstance(compressor, gribscan.rawgribcodec.RawGribCodec):
            ds[dv].attrs["original_compressor"]="gribscan"
            if ds[dv].encoding["dtype"] == np.float64:
                ds[dv] = ds[dv].astype("float32")
    
    return ds


def lossy_compress_chunk(partds) :
    """
    Apply lossy compression to dataset using BitRound.

    This function applies BitRound compression with 10 keepbits to the input data array,
    which reduces precision while maintaining reasonable accuracy.

    Args:
        partds (xr.Dataset): Input dataset to compress

    Returns:
        np.Array: Compressed dataset

    """
#    import numcodecs
#    rounding = numcodecs.BitRound(keepbits=10)
    return rounding.decode(rounding.encode(partds))


def apply_lossy_compression(
    ds: xr.Dataset, L_DASK: bool = True
) -> xr.Dataset:
    """
    Apply lossy compression to dataset chunks and store in Zarr format.

    This function compresses dataset chunks using Zstd compression and stores
    them in Zarr format. Supports both Dask and non-Dask operations.

    Args:
        ds (xr.Dataset): Dataset to compress
        L_DASK (bool): Whether to use Dask for parallel processing

    """
    
    se=ds.encoding.get("source")
#    if L_DASK:    
#        ds=xr.apply_ufunc(
#                lossy_compress_chunk,
#                ds,
#                dask="parallelized",
#                keep_attrs="drop_conflicts"
#                )
#    else:
#        for var in ds.data_vars:
#            ds[var].encoding["filters"]=numcodecs.BitRound(keepbits=12)            
    for var in ds.data_vars:
        if isinstance(ds[var].data, Daarray):
            ds[var].encoding["filters"]=[numcodecs.BitRound(keepbits=10)]
    ds.encoding["source"]=se         
    
    return ds

def set_compression(ds: xr.Dataset) -> xr.Dataset:
    """
    Set compression for dataset variables.

    This function sets the compressor for all variables in the input dataset
    to Blosc with LZ4 compression and level 5.

    Args:
        ds (xr.Dataset): Input dataset to compress

    Returns:
        xr.Dataset: Dataset with compression applied
    """
    for var in ds.variables:
        if isinstance(ds[var].data, Daarray):
            ds[var].encoding["compressor"] = numcodecs.Blosc(
                    cname="lz4", clevel=5, shuffle=2
                )  # , blocksize=0),
        else:
            ds[var].encoding["compressor"] = None
    return ds


def get_combination_list(ups: dict) -> list[dict]:
    """
    Generate all possible combinations of update parameters.

    This function creates a list of dictionaries containing all possible combinations
    of values for the given update parameters.

    Args:
        ups (dict): Dictionary of update parameters where each value contains:
            - "name": Parameter name
            - "allowed": List of allowed values

    Returns:
        list[dict]: List of dictionaries with all parameter combinations
    """
    if not ups:
        return []
    
    uplists = [up["allowed"] for up in ups]
    combinations = list(itertools.product(*uplists))
    return [{ups[i]["name"]: comb[i] for i in range(len(ups))} for comb in combinations]

def dotted_get(cat, key):
    parts = key.split(".")
    testcat = cat
    realp = ""
    for p in parts:
        if not realp:
            realp = p
        else:
            realp = realp + "." + p
        if realp in testcat.entries:
            if p != parts[-1]:
                testcat = testcat[realp].read()
                realp = ""
            else:
                return testcat[realp]

    raise ValueError(f"Could not find '{realp}' in catalog")

def find_data_sources(
    catalog: intake.Catalog, name: str = None
) -> list[str]:
    """
    Recursively find data sources in an intake catalog.

    This function searches through an intake catalog and its subcatalogs to find
    all data sources, excluding CSV sources and .nc files.

    Args:
        catalog (intake.Catalog): Intake catalog to search
        name (str, optional): Base name to prepend to source names

    Returns:
        list[str]: List of data source names with their full paths
    """
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
            
        elif isinstance(entry, intake.source.base.DataSource) and not key.endswith(".nc"):
            data_sources.append(newname + "." + key)
    
    return data_sources

def find_data_sources_v2(
    catalog: intake.Catalog, name: str = None
) -> list[str]:

    data_sources = []

    for key in catalog.entries:
        entry = catalog[key]
        newname='.'.join([name,key])
        if isinstance(entry, intake.readers.readers.XArrayDatasetReader):
            data_sources.append(newname)
        elif isinstance(entry, intake.readers.readers.YAMLCatalogReader):
            data_sources.extend(find_data_sources_v2(entry.read(), newname))

    return data_sources

def get_dataset_dict_from_intake(
    cat,
    dsnames: list,
    prefix: str = None,
    drop_vars: list = None,
    whitelist_paths: list = None,
    storage_chunk_patterns: list = None,
    allowed_ups: dict = None,
    mdupdate: bool = False,
    l_dask: bool = True,
    cache_size: int = 128
):
    """
    Load datasets from intake catalog with configurable options.

    This function loads datasets from an intake catalog with various configuration
    options for chunking, variable dropping, and path filtering. It's used by
    multiple cloudify scripts (cloudify_era5.py, cloudify_dyamond.py,
    cloudify_eerie.py, cloudify_cosmorea.py) for dataset loading.

    Args:
        cat: Intake catalog object containing the datasets
        dsnames (list): List of dataset names to load
        prefix (str, optional): Prefix to add to dataset names
        drop_vars (list, optional): Variables to drop from datasets
        whitelist_paths (list, optional): List of paths to allow
        storage_chunk_patterns (list, optional): Patterns for storage chunking
        allowed_ups (dict, optional): Allowed user parameters
        mdupdate (bool, optional): Whether to update metadata

    Returns:
        dict: Dictionary mapping dataset names to xarray.Dataset objects

    Raises:
        ValueError: If dataset loading fails
    """
    dsdict = {}
    mapper_dict = {}
    for dsname in dsnames:
        print(dsname)
        desc = dotted_get(cat,dsname).to_dict()
        kwargs = desc.get("kwargs")
        ups = desc.get("user_parameters")
        if allowed_ups:
            for upuser, upvals in allowed_ups.items():
                for idx, up in enumerate(ups):
                    if up["name"] == upuser:
                        ups[idx]["allowed"] = upvals
        md = desc.get("metadata")
        args = kwargs.get("args")[0]
        urlpath = args.get("url")
        if type(urlpath) == list:
            urlpath = urlpath[0]
        if whitelist_paths:
            if not any(a in urlpath for a in whitelist_paths):
                print("Not in known projects:")
                print(mdsid)
                continue
        dictname = dsname if not prefix else prefix + dsname

        upnames = []
        if ups:
            upnames = [a["name"] for a in ups]
        comb = {"zarr_format":kwargs.get("zarr_format")}
        if args.get("storage_options"):
            comb["storage_options"]=args.get("storage_options")
            if not comb["storage_options"].get("cache_size"):
                comb["storage_options"]["cache_size"]=cache_size
        if not l_dask:
            comb["chunks"]=None
        else:
            if storage_chunk_patterns:
                if any(b in dsname for b in storage_chunk_patterns):
                    comb["chunks"] = {}
            elif kwargs.get("chunks"):
                comb["chunks"] = kwargs.get("chunks")
            if comb.get("chunks","notset") == "notset":
                comb["chunks"] = "auto"
        if drop_vars and not urlpath.endswith(".nc"):  # not possible for nc sources
            if type(drop_vars) == dict:
                b = next((b for b in drop_vars.keys() if b in dsname), None)
                if b:
                    comb["drop_variables"] = drop_vars[b]
            else:
                comb["drop_variables"] = drop_vars
        if (
            ups
            and any(up in desc["args"]["urlpath"] for up in upnames)
            and not upnames == ["method"]
        ):
            combination_list = get_combination_list(ups)
            for combl in combination_list:
                iakey = dictname + "." + "_".join([str(b) for b in combl.values()])
                if l_dask:
                    comb["chunks"] = {}
#                comb.update(combl)
                urlpath=dotted_get(cat,dsname)(**combl).urlpath
                if urlpath.startswith("reference"):
                    comb["consolidated"]=False
                elif args.get("consolidated"):
                    comb["consolidated"]=args.get("consolidated")               
                try:
#                if True:
                    #ds = cat[dsname](**comb).to_dask()
                    ds,mapper = open_zarr_and_mapper(urlpath, **comb)
                    ds.encoding["source"]=urlpath
                    dsdict[dictname] = ds
                    mapper_dict[urlpath] = mapper
                    if mdupdate:
                        ds.attrs.update(md)
                    # chunks="auto",storage_options=storage_options).to_dask()
                    ds.attrs["href"] = (
                        ds.encoding["source"] if ds.encoding["source"] else urlpath
                    )
                    ds.attrs["open_kwargs"] = copy(args)
                    if l_dask:
                        ds.attrs["total_no_of_chunks"] = sum(
                            np.prod(var.data.numblocks)
                            for var in ds.data_vars.values()
                            if hasattr(var.data, "numblocks")
                        )
                    del ds.attrs["open_kwargs"]["urlpath"]
                    ds.attrs["open_kwargs"].update(dict(engine="zarr"))
                    dsdict[iakey] = ds

                except:
                    print("Could not open " + iakey)
                    continue
        else:
            try:
#            if True:
                if urlpath.startswith("reference"):
                    comb["consolidated"]=False
                elif args.get("consolidated"):
                    comb["consolidated"]=args.get("consolidated")
                print(urlpath)
                print(comb)
                ds,mapper = open_zarr_and_mapper(urlpath, **comb)                
#                ds = cat[dsname](**comb).to_dask()
                if mdupdate:
                    ds.attrs.update(md)
                ds.encoding["source"]=urlpath
                dsdict[dictname] = ds
                mapper_dict[urlpath] = mapper
                # chunks="auto",storage_options=storage_options).to_dask()
            except:
                print("Could not open " + dsname)
                continue
    return mapper_dict, dsdict


def reset_encoding_get_mapper(
    mapper_dict: dict, dsid: str, ds: xr.Dataset, desc: dict = None, l_dask: bool=True
) -> tuple[dict, xr.Dataset]:
    """
    Drop dataset encoding and get mapper for storage.

    This function drops the dataset's encoding and sets up a mapper for storage
    using appropriate storage options.

    Args:
        mapper_dict (dict): Dictionary mapping sources to mappers
        dsid (str): Dataset ID
        ds (xr.Dataset): Input dataset
        desc (dict, optional): Description dictionary containing storage options
        l_dask (bool, optional): Switch for necessary settings when using dask

    Returns:
        tuple[dict, xr.Dataset]: Updated mapper dictionary and dataset

    Raises:
        ValueError: If input parameters are invalid
    """

    sp = ds.encoding.get("source")
    if (sp is None) and desc:
        updesc = desc["args"]["urlpath"]
        if isinstance(updesc, str) or (isinstance(updesc, list) and len(updesc) == 1):
            if isinstance(updesc, list):
                updesc = updesc[0]
            sp = updesc
    
    if l_dask:
        ds = ds.drop_encoding()
    
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
def set_frequency(inp: str, ds: xr.Dataset) -> xr.Dataset:
    """
    Set frequency attribute based on input string.

    This function sets the 'frequency' attribute on the dataset based on matching
    keywords in the input string. Uses the frequency_mapping dictionary to map
    keywords to standard frequency codes.

    Args:
        inp (str): Input string to search for frequency keywords
        ds (xr.Dataset): Dataset to modify

    Returns:
        xr.Dataset: Dataset with frequency attribute set

    Raises:
        ValueError: If input parameters are invalid
    """
    for keyword, frequency in frequency_mapping.items():
        if keyword in inp:
            ds.attrs["frequency"] = frequency
            break
    else:
        ds.attrs["frequency"] = "unknown"  # Default value if no match is found
    return ds


def adapt_for_zarr_plugin_and_stac(dsid: str, ds: xr.Dataset) -> xr.Dataset:
    """
    Adapt dataset attributes for Zarr plugin and STAC compatibility.

    This function updates dataset attributes to make it compatible with Zarr plugin
    and STAC specifications. It performs several key operations:
    1. Sets proper title using dataset ID
    2. Creates a descriptive dataset description
    3. Handles time variable encoding and metadata
    4. Sets frequency and creation date attributes

    Args:
        dsid (str): Dataset ID used to determine frequency
        ds (xr.Dataset): Input dataset to modify

    Returns:
        xr.Dataset: Dataset with updated attributes for Zarr/STAC compatibility

    Raises:
        ValueError: If input parameters are invalid
    """
    
    # Update title if default
    title = ds.attrs.get("title", "default")
    if title in ["default", "ICON simulation"]:
        ds.attrs["title"] = dsid
    
    # Set description if not present
    if not ds.attrs.get("description"):
        # Get source information
        source = next(
            (ds.attrs.get(key) for key in SOURCE_KEYS if ds.attrs.get(key) is not None),
            "not Set"
        )
        
        exp = next(
            (ds.attrs.get(key) for key in EXPERIMENT_KEYS if ds.attrs.get(key) is not None),
            "not Set"
        )
        
        project = next(
            (ds.attrs.get(key) for key in PROJECT_KEYS if ds.attrs.get(key) is not None),
            "not Set"
        )
        
        institute = next(
            (ds.attrs.get(key) for key in INSTITUTE_KEYS if ds.attrs.get(key) is not None),
            "not Set"
        )
        
        ds.attrs["description"] = (
            f"Simulation data from project '{project}' produced by Earth System Model '{source}' "
            f"and run by institution '{institute}' for the experiment '{exp}'"
        )
    
    # Handle time variable if present
    if "time" in ds.variables:
        if ds["time"].encoding.get("_FillValue"):
            del ds["time"].encoding["_FillValue"]
        
        ds["time"].encoding["dtype"] = "float64"
        ds["time"].encoding["compressor"] = None
        ds.attrs["time_min"] = str(ds["time"].values[0])
        ds.attrs["time_max"] = str(ds["time"].values[-1])
        
        # Move time attributes to encoding
        for att in ["units", "calendar"]:
            if ds["time"].attrs.get(att) and not ds["time"].encoding.get(att):
                ds["time"].encoding[att] = ds["time"].attrs[att]
                del ds["time"].attrs[att]
    else:  # for freva
        if not ds.attrs.get("time_min"):
            ds.attrs["time_min"] = None
        if not ds.attrs.get("time_max"):
            ds.attrs["time_max"] = None
    
    # Set frequency and creation date
    ds = set_frequency(dsid.split(".")[-1].lower(), ds)
    ds.attrs["creation_date"] = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return ds
