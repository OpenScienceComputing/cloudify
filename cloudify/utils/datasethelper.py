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

def set_custom_header(response: fastapi.Response) -> None:
    response.headers["Cache-control"] = "max-age=3600"
    response.headers["X-EERIE-Request-Id"] = "True"
    response.headers["Last-Modified"] = datetime.today().strftime(
        "%a, %d %b %Y 00:00:00 GMT"
    )


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
            if ds[dv].encoding["dtype"] == np.float64:
                ds[dv] = ds[dv].astype("float32")
    
    return ds


def lossy_compress_chunk(partds) :
    """
    Apply lossy compression to dataset using BitRound.

    This function applies BitRound compression with 12 keepbits to the input data array,
    which reduces precision while maintaining reasonable accuracy.

    Args:
        partds (xr.Dataset): Input dataset to compress

    Returns:
        np.Array: Compressed dataset

    """
    import numcodecs
    rounding = numcodecs.BitRound(keepbits=12)
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
    if L_DASK:    
        ds=xr.apply_ufunc(
                lossy_compress_chunk,
                ds,
                dask="parallelized",
                keep_attrs="drop_conflicts"
                )
    else:
        for var in ds.data_vars:
            ds[var].encoding["filters"]=numcodecs.BitRound(keepbits=12)            
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
    for var in ds.data_vars:
        ds[var].encoding = {
            "compressor": numcodecs.Blosc(
                cname="lz4", clevel=5, shuffle=2
            )  # , blocksize=0),
        }
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


def get_dataset_dict_from_intake(
    cat,
    dsnames: list,
    prefix: str = None,
    drop_vars: list = None,
    whitelist_paths: list = None,
    storage_chunk_patterns: list = None,
    allowed_ups: dict = None,
    mdupdate: bool = False,
    l_dask: bool = True
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
    for dsname in dsnames:
        print(dsname)
        desc = cat[dsname].describe()
        ups = desc.get("user_parameters")
        if allowed_ups:
            for upuser, upvals in allowed_ups.items():
                for idx, up in enumerate(ups):
                    if up["name"] == upuser:
                        ups[idx]["allowed"] = upvals
        md = desc.get("metadata")
        args = desc.get("args")
        urlpath = args.get("urlpath")
        if type(urlpath) == list:
            urlpath = urlpath[0]
        if whitelist_paths:
            if not any(a in urlpath for a in whitelist_paths):
                print("Not in known projects:")
                print(mdsid)
                continue
        dictname = dsname if not prefix else prefix + dsname

        upnames = [a["name"] for a in ups]
        comb = {}
        if not l_dask:
            comb["chunks"]=None
        else:
            if storage_chunk_patterns:
                if any(b in dsname for b in storage_chunk_patterns):
                    comb["chunks"] = {}
            if not args.get("chunks") and not comb.get("chunks"):
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
                comb["chunks"] = {}
                if not l_dask:
                    comb["chunks"] = None
                comb.update(combl)
                try:
                    ds = cat[dsname](**comb).to_dask()
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
                ds = cat[dsname](**comb).to_dask()
                if mdupdate:
                    ds.attrs.update(md)
                dsdict[dictname] = ds
                # chunks="auto",storage_options=storage_options).to_dask()
            except:
                print("Could not open " + dsname)
                continue
    return dsdict


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
