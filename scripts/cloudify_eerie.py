import intake
from datasethelper import *


def add_eerie(mapper_dict: dict, dsdict: dict) -> (dict, dict):
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
    l_dask = True

    cat = intake.open_catalog(source_catalog)
    dsone = {}
    for realm in ["atmos", "ocean"]:
        dsone[f"icon_{realm}_native_grid"] = (
            cat[
                f"model-output.icon-esm-er.hist-1950.v20240618.{realm}.native.2d_daily_mean"
            ](chunks=None)
            .to_dask()
            .reset_coords()[["lat", "lon", "cell_sea_land_mask"]]
            .chunk()
        )
    hostids = []
    for source in list(cat["model-output"]):
        if source != "csv" and source != "esm-json":
            hostids += find_data_sources(cat["model-output"][source])

    for mdsid in hostids:
        print(mdsid)
        if "icon" in mdsid and "v2023" in mdsid:
            continue
        desc = None
        listref = False
        desc = cat["model-output"][mdsid].describe()
        args = desc["args"]
        testurl = args["urlpath"]
        if type(testurl) == list:
            testurl = testurl[0]
            listref = True
        if not any(a in testurl for a in ["bm1344", "bm1235", "bk1377"]):
            print("Not in known projects:")
            print(mdsid)
            continue
        if "3d_grid" in mdsid:
            continue
        if not "reference::" in testurl:
            print(testurl)
            continue
        localdsdict = {}
        print("Try to open: " + mdsid)
        try:
            # if True:
            options = get_dask_options(desc)
            ups = desc.get("user_parameters")
            if not args.get("chunks") and not options.get("chunks"):
                options["chunks"] = "auto"
            if args.get("chunks") and type(args.get("chunks")) == dict:
                print("HA!")
                print(args.get("chunks"))
            if (
                (
                    "icon" in mdsid
                    and "native" in mdsid
                    and (
                        "2d_monthly_mean" in mdsid
                        or "5lev" in mdsid
                        or "model-level" in mdsid
                        or ".mon" in mdsid
                    )
                )
                or "grid" in mdsid
                or "healpix" in mdsid
            ):
                options["chunks"] = {}
            if "icon" in mdsid and "native" in mdsid:
                options["drop_variables"] = ["lat,lon", "cell_sea_land_mask"]
            print(options)
            if ups and any(b["name"] == "zoom" for b in ups):
                combination_list = get_combination_list(ups)
                print(combination_list)
                options["drop_variables"] = ["lat", "lon"]
                for comb in combination_list:
                    ldsid = (
                        mdsid + "." + "_".join([str(a) for a in list(comb.values())])
                    )
                    localdsdict[ldsid] = cat["model-output"][mdsid](
                        **options, **comb
                    ).to_dask()
            else:
                localdsdict[mdsid] = cat["model-output"][mdsid](**options).to_dask()
        except Exception as e:
            # else:
            print("Could not load:")
            print(e)
            continue
        for dsid, ds in localdsdict.items():
            ds.attrs["_catalog_id"] = dsid
            if "icon" in dsid and "native" in dsid:
                for l in ["lat", "lon", "cell_sea_land_mask"]:
                    if "ocean" in dsid:
                        if l in ds.variables:
                            del ds[l]
                        ds[l] = dsone["icon_ocean_native_grid"][l]
                        ds = ds.set_coords(l)
                    if "atmos" in dsid:
                        if l in ds.variables:
                            del ds[l]
                        ds[l] = dsone["icon_atmos_native_grid"][l]
                        ds = ds.set_coords(l)
            if "d" in ds.data_vars:
                ds = ds.rename(d="testd")

            if "fesom" in dsid:  #            and "ocean" in dsid and "native" in dsid:
                if not l_dask:
                    continue
                ds = chunk_and_prepare_fesom(ds)
            if "hadgem" in dsid:
                droplist = [a for a in ["height"] if a in ds.variables]
                if droplist:
                    ds = ds.drop(droplist)
            ds = set_or_delete_coords(ds, dsid)
            ds = gribscan_to_float(ds)
            # if dsid.startswith("ifs-amip"):
            if not "grid" in dsid:
                if "native" in dsid:
                    mapper_dict, ds = apply_lossy_compression(
                        mapper_dict, dsid, ds, l_dask
                    )
                ds = set_compression(ds)

            if listref:
                splitted_ds = split_ds(ds)
                for idx, dss in enumerate(splitted_ds):
                    newid = dsid + f".{idx}"
                    dss = adapt_for_zarr_plugin_and_stac(dsid, dss)
                    dsdict[newid] = dss
            else:
                if l_dask:
                    mapper_dict, ds = reset_encoding_get_mapper(
                        mapper_dict, dsid, ds, desc=desc
                    )
                ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
                dsdict[dsid] = ds
    return mapper_dict, dsdict
