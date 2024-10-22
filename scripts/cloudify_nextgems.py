from datasethelper import *
import xarray as xr
import intake
import yaml
import itertools

NGC_PROD_CAT = "https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml"
#    NGC_PROD_CAT="https://data.nextgems-h2020.eu/catalog.yaml"
DS_ADD = [
    "ICON.ngc4008",
    "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512",
    "IFS.IFS_9-FESOM_5-production.3D_hourly_healpix512",
]


def refine_nextgems(iakey, ds, md, desc):
    ds = reset_encoding_get_mapper(iakey, ds, desc=desc)
    ds = adapt_for_zarr_plugin_and_stac(iakey, ds)
    ds = set_compression(ds)
    for mdk, mdv in md.items():
        if not mdk in ds.attrs:
            ds.attrs[mdk] = mdv
    for mdk, mdv in desc["metadata"].items():
        if not mdk in ds.attrs:
            ds.attrs[mdk] = mdv
    return ds


def add_nextgems(dsdict):
    ngccat = intake.open_catalog(NGC_PROD_CAT)
    md = yaml.safe_load(ngccat.yaml())["sources"]["nextGEMS_prod"]["metadata"]
    for ia in DS_ADD:
        desc = ngccat[ia].describe()
        if type(desc["args"]["urlpath"]) == str and desc["args"]["urlpath"].endswith(
            "zarr"
        ):
            if "user_parameters" in desc:
                ups = desc["user_parameters"]
                uplists = [up["allowed"] for up in ups]
                combinations = list(itertools.product(*uplists))
                combdict = [
                    {ups[i]["name"]: comb[i] for i in range(len(ups))}
                    for comb in combinations
                ]
                for comb in combdict:
                    iakey = (
                        "nextgems."
                        + ia
                        + "."
                        + "_".join([str(a) for a in list(comb.values())])
                    )
                    try:
                        dsdict[iakey] = ngccat[ia](**comb, chunks="auto").to_dask()
                        dsdict[iakey] = refine_nextgems(iakey, dsdict[iakey], md, desc)
                    except Exception as e:
                        print(e)
                        pass
        else:
            iakey = "nextgems." + ".".join(ia.split(".")[1:])
            dsdict[iakey] = ngccat[ia](chunks="auto").to_dask()
            dsdict[iakey] = refine_nextgems(iakey, dsdict[iakey], md, desc)
    print(dsdict.keys())
    return dsdict
