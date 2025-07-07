from cloudify.utils.datasethelper import *
from copy import deepcopy as copy
import xarray as xr
import intake
import yaml
import itertools
import fsspec


def test_catalog(incat: str) -> str:
    try:
        fsspec.open(incat).open()
    except Exception as e:
        print(e)
        return None
    return incat


def refine_nextgems(
    mapper_dict: dict, iakey: str, ds: xr.Dataset, desc: dict
) -> (dict, xr.Dataset):
    md = desc.get("metadata")
    mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, iakey, ds, desc=desc)
    print("adapt for zarr")
    ds = adapt_for_zarr_plugin_and_stac(iakey, ds)
    print("Set compression")
    ds = set_compression(ds)
    for mdk, mdv in md.items():
        if not mdk in ds.attrs:
            ds.attrs[mdk] = mdv
    if desc.get("metadata"):
        for mdk, mdv in desc["metadata"].items():
            if not mdk in ds.attrs:
                ds.attrs[mdk] = mdv
    #    print("Lets remove encoding")
    #    for var in ds.variables:
    #        del ds[var].encoding["chunks"]
    return mapper_dict, ds


def get_args(desc: dict) -> dict:
    args = copy(desc["args"])
    if not args.get("storage_options"):
        args["storage_options"] = {}
    if type(args["urlpath"]) == str:
        if args["urlpath"].startswith("reference") and not args["storage_options"].get(
            "remote_protocol", None
        ):
            args["storage_options"].update(dict(lazy=True, remote_protocol="file"))
    #    if not args.get("chunks"):
    args["chunks"] = "auto"
    del args["urlpath"]
    return args


def add_healpix(i: str, v: xr.Dataset) -> xr.Dataset:
    crs = xr.open_zarr(
        "/work/bm1235/k202186/dy3ha-rechunked/d3hp003.zarr/PT1H_inst_z0_atm"
    )[["crs"]]
    levstr = i.split("healpix")[1].split(".")[0].split("_")[0]
    try:
        lev = int(levstr)
    except:
        print(levstr)
        print(i)
        print("not healpix level")
        return v
    v["crs"] = crs["crs"]
    v["crs"].attrs["healpix_nside"] = lev
    for dv in v.data_vars:
        v[dv].attrs["grid_mapping"] = "crs"
    return v


def add_nextgems(mapper_dict: dict, dsdict: dict):
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_nextgems/catalog.yaml"
    published_catalog = "https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml"
    DS_ADD = ["ICON.ngc5004"]  # , "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512"]
    DS_ADD_SIM = [
        "IFS.IFS_2.8-FESOM_5-production-parq",
        "IFS.IFS_2.8-FESOM_5-production-deep-off-parq",
    ]

    ngccat = intake.open_catalog(source_catalog)

    ngc4_md=None
    
    try:
        prodcat = intake.open_catalog(published_catalog)
        ngc4_md = yaml.safe_load(prodcat.yaml())["sources"]["nextGEMS_prod"]["metadata"]        
    except:
        print("wdcc not working")

    gr_025 = (
        ngccat["IFS.IFS_2.8-FESOM_5-production-parq"]["2D_monthly_0.25deg"]
        .to_dask()
        .reset_coords()[["lat", "lon"]].load()
    )
    
    all_ds = copy(DS_ADD)
    for sim in DS_ADD_SIM:
        for dsn in list(ngccat[sim]):
            all_ds.append(sim + "." + dsn)
            
    descdict={}
                                             
    localdsdict=get_dataset_dict_from_intake(
        ngccat, 
        all_ds, 
        prefix="nextgems.",
        storage_chunk_patterns=["2048"],
        drop_vars={
            "25deg":["lat", "lon"]
        }
    )

    for dsn in list(localdsdict.keys()):
        iakey=dsn.replace("-parq","")
        desckey=iakey.replace('nextgems.','')
        localdsdict[iakey]=localdsdict.pop(dsn)        
        
        try:
            descdict[iakey]=ngccat[desckey].describe()
        except:
            descdict[iakey]=ngccat['.'.join(desckey.split('.')[:-1])].describe()
        if "ngc4008" in dsn or "IFS_9-FESOM_5-production" in dsn:
            descdict[iakey]["metadata"] = ngc4_md                
            
    for ia, ds in localdsdict.items():
    #for ia in all_ds:
        print(ia)
        print("gribscan to float")
        ds = gribscan_to_float(ds)
        mapper_dict, ds = refine_nextgems(
            mapper_dict, ia, ds, descdict[ia]
        )
        if "25deg" in ia:
            ds.coords["lat"] = gr_025["lat"]
            ds.coords["lon"] = gr_025["lon"]
        print("try to set crs")
        if "crs" in ds.variables:
            if len(str(ds["crs"].attrs.get("healpix_nside", "No"))) >= 4:
                mapper_dict, ds = apply_lossy_compression(
                    mapper_dict, ia, ds
                )
        elif "healpix" in ia :
            ds = add_healpix(ia,ds)
        dsdict[ia]=ds
    del localdsdict
    return mapper_dict, dsdict
