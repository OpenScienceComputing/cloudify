from datasethelper import *
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
    mapper_dict: dict, iakey: str, ds: xr.Dataset, md: dict, desc: dict
) -> (dict, xr.Dataset):
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

    # caturl=test_catalog(source_catalog)
    # if not caturl:
    #     return mapper_dict, dsdict
    ngccat = intake.open_catalog(source_catalog)

    #  ngc4_md=None
    #  caturl=test_catalog(published_catalog)

    #   print("tested2")

    #   if caturl:
    prodcat = intake.open_catalog(published_catalog)
    ngc4_md = yaml.safe_load(prodcat.yaml())["sources"]["nextGEMS_prod"]["metadata"]

    print("safe loaded")

    gr_025 = (
        ngccat["IFS.IFS_2.8-FESOM_5-production-parq"]["2D_monthly_0.25deg"]
        .to_dask()
        .reset_coords()[["lat", "lon"]]
    )
    gr_025 = gr_025.load()

    all_ds = copy(DS_ADD)
    for sim in DS_ADD_SIM:
        for dsn in list(ngccat[sim]):
            all_ds.append(sim + "." + dsn)
    print("loop over sims")

    for ia in all_ds:
        print(ia)
        desc = ngccat[ia].describe()
        #        if type(desc["args"]["urlpath"])==str and desc["args"]["urlpath"].endswith("zarr"):
        ups = desc.get("user_parameters")
        args = get_args(desc)
        print(args)
        md = desc.get("metadata")
        if "ngc4008" in ia or "IFS_9-FESOM_5-production" in ia:
            md = ngc4_md
        if ups:
            combination_list = get_combination_list(ups)
            for comb in combination_list:
                iakey = (
                    "nextgems."
                    + ia
                    + "."
                    + "_".join([str(a) for a in list(comb.values())])
                )
                iakey = iakey.replace("-parq", "")
                try:
                    print(args)
                    dslocal = ngccat[ia](
                        **comb, **args
                    ).to_dask()  # chunks="auto",storage_options=storage_options).to_dask()
                    print("gribscan to float")
                    dslocal = gribscan_to_float(dslocal)
                    # if "crs" in dsdict[iakey].variables:
                    #    if len(str(dsdict[iakey]["crs"].attrs.get("healpix_nside","No"))) >= 4:
                    #        mapper_dict, dsdict[iakey] = apply_lossy_compression(mapper_dict,iakey,dsdict[iakey])
                    mapper_dict, dsdict[iakey] = refine_nextgems(
                        mapper_dict, iakey, dslocal, md, desc
                    )
                except Exception as e:
                    print(e)
                    pass
        else:
            iakey = "nextgems." + ".".join(ia.split(".")[1:])
            iakey = iakey.replace("-parq", "")
            try:
                args = desc.get("args", {})
                #                if not args:
                args["chunks"] = "auto"
                #                if "healpix" in iakey:
                args["drop_variables"] = ["lat", "lon"]
                if "2048" in iakey:
                    args["chunks"] = {}
                print(iakey)
                dslocal = ngccat[ia](**args).to_dask()
                print("gribscan to float")
                dslocal = gribscan_to_float(dslocal)
                if "25deg" in iakey:
                    dslocal["lat"] = gr_025["lat"]
                    dslocal["lon"] = gr_025["lon"]
                    dslocal = dslocal.set_coords(["lat", "lon"])
                print("try to set crs")
                if "crs" in dslocal.variables:
                    if len(str(dslocal["crs"].attrs.get("healpix_nside", "No"))) >= 4:
                        mapper_dict, dslocal = apply_lossy_compression(
                            mapper_dict, iakey, dslocal
                        )
                mapper_dict, dsdict[iakey] = refine_nextgems(
                    mapper_dict, iakey, dslocal, md, desc
                )
            except:
                print(iakey + " does not work")
                pass
    #
    print(dsdict.keys())
    for i, v in dsdict.items():
        if "healpix" in i and not "crs" in list(v.variables):
            v = add_healpix(i, v)
    return mapper_dict, dsdict
