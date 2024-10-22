import numcodecs
import intake
import fsspec
import xarray as xr
import datetime

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


def set_compression(ds):
    for var in ds.data_vars:
        #                ds[var].encoding["compressor"]=None
        ds[var].encoding = {
            "compressor": numcodecs.Blosc(
                cname="lz4", clevel=5, shuffle=1, blocksize=0
            ),
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


def reset_encoding_get_mapper(dsid, ds, desc=None):
    global mapper_dict
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
            use_options.update(desc["args"].get("storage_options", {}))
        mapper_dict[sp] = fsspec.get_mapper(sp, **use_options)
        ds.encoding["source"] = sp
    return ds


def adapt_for_zarr_plugin_and_stac(dsid, ds):
    title = ds.attrs.get("title", "default")
    if title in ["default", "ICON simulation"]:
        ds.attrs["title"] = dsid
    desc = ds.attrs.get("description", None)
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
        ds["time"].encoding["dtype"] = "float64"
        ds["time"].encoding["compressor"] = None
        ds.attrs["time_min"] = str(ds["time"].values[0])
        ds.attrs["time_max"] = str(ds["time"].values[-1])
        for att in ["units", "calendar"]:
            if ds["time"].attrs.get(att, None) and not ds["time"].encoding.get(
                att, None
            ):
                ds["time"].encoding[att] = ds["time"].attrs[att]
                del ds["time"].attrs[att]
    ds.attrs["creation_date"] = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
    return ds
