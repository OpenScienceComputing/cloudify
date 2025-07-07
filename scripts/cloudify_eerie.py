import intake
from cloudify.utils.datasethelper import *


def get_dsone(cat, model: str, realm: str, coords: list):
    for dsname in ["2d_monthly_mean", "monthly_node", "2D_monthly_avg"]:
        try:
            dscoords = (
                cat[f"model-output.{model}.{realm}.native.{dsname}"](chunks=None)
                .to_dask()
                .reset_coords()[coords]
                .chunk()
            )
            return dscoords
        except:
            pass

    return None


def add_eerie(mapper_dict: dict, dsdict: dict) -> (dict, dict):
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
    l_dask = True

    cat = intake.open_catalog(source_catalog)
    dsonedict = {
        "icon_atmos": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "atmos",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "icon_ocean": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "ocean",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "icon_land": get_dsone(
            cat,
            "icon-esm-er.hist-1950.v20240618",
            "atmos",
            ["lat", "lon", "cell_sea_land_mask"],
        ),
        "ifs-fesom_atmos": get_dsone(
            cat, "ifs-fesom2-sr.hist-1950.v20240304", "atmos", ["lat", "lon"]
        ),
        "ifs-fesom_ocean": get_dsone(
            cat,
            "ifs-fesom2-sr.eerie-spinup-1950.v20240304",
            "ocean",
            ["lat", "lon", "coast"],
        ),
        "ifs-amip-tco1279_atmos": get_dsone(
            cat, "ifs-fesom2-sr.hist-1950.v20240304", "atmos", ["lat", "lon"]
        ),
    }

    hostids = []
    for source in list(cat["model-output"]):
        if source != "csv" and source != "esm-json":
            hostids += [
                a
                for a in find_data_sources(cat["model-output"][source])
                if not any(b in a for b in ["v2023", "3d_grid"])
            ]

    localdsdict = get_dataset_dict_from_intake(
        cat["model-output"],
        hostids,
        whitelist_paths=["bm1344", "bm1235", "bk1377"],
        storage_chunk_patterns=["native", "grid", "healpix", ".mon"],
        drop_vars=dict(
            native=["lat", "lon", "cell_sea_land_mask"], healpix=["lat", "lon"]
        ),
    )

    for dsid, ds in localdsdict.items():
        ds.attrs["_catalog_id"] = dsid
        if "native" in dsid:
            dsone = dsonedict.get(
                next(
                    (
                        k
                        for k in dsonedict.keys()
                        if all(b in dsid for b in k.split("_"))
                    ),
                    "nothing_found",
                )
            )
            if dsone and not "elem" in dsid:
                try:
                    for dv in dsone.variables.keys():
                        ds.coords[dv] = dsone[dv]
                except:
                    print(f"Couldnt set coordinates for {dsid}")
            else:
                print(f"Couldnt find coords for {dsid}")
        if "d" in ds.data_vars:
            ds = ds.rename(d="testd")

        if "fesom" in dsid:  #            and "ocean" in dsid and "native" in dsid:
            if not l_dask:
                continue
            # ds = chunk_and_prepare_fesom(ds)
            if "heightAboveGround" in ds.variables:
                ds = ds.drop("heightAboveGround")

        if "hadgem" in dsid:
            droplist = [a for a in ["height"] if a in ds.variables]
            if droplist:
                ds = ds.drop(droplist)
        ds = set_or_delete_coords(ds, dsid)
        ds = gribscan_to_float(ds)

        # lossy has to come first!
        if not "grid" in dsid:
            if "native" in dsid or "_11" in dsid or "_10" in dsid:
                mapper_dict, ds = apply_lossy_compression(mapper_dict, dsid, ds, l_dask)
        if l_dask:
            mapper_dict, ds = reset_encoding_get_mapper(
                mapper_dict,
                dsid,
                ds,  # desc=desc
            )
        ds = adapt_for_zarr_plugin_and_stac(dsid, ds)
        ds = set_compression(ds)
        dsdict[dsid] = ds
    del localdsdict

    return mapper_dict, dsdict
