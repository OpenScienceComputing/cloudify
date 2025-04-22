from datasethelper import *
import xarray as xr
import intake
import yaml
import itertools
import fsspec

def test_catalog():
    NGC_PROD_CAT="https://raw.githubusercontent.com/nextGEMS/catalog/refs/heads/main/catalog.yaml"
#    NGC_PROD_CAT="/opt/nextgems_catalog.yaml"
#    NGC_PROD_CAT="https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml"
    try:
        fsspec.open(NGC_PROD_CAT).open()
    except Exception as e:
        print(e)
        #NGC_PROD_CAT="https://data.nextgems-h2020.eu/catalog.yaml"
        return None
    return NGC_PROD_CAT

DS_ADD=["ICON.ngc4008", "ICON.ngc5004", "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512",
#DS_ADD=["ICON.ngc4008", "IFS.IFS_9-FESOM_5-production.2D_hourly_healpix512",
            "IFS.IFS_9-FESOM_5-production.3D_hourly_healpix512"]
def refine_nextgems(mapper_dict, iakey, ds,md,desc):
    mapper_dict, ds=reset_encoding_get_mapper(mapper_dict,iakey, ds,desc=desc)
    ds=adapt_for_zarr_plugin_and_stac(iakey,ds)
    ds = set_compression(ds)
    for mdk,mdv in md.items():
        if not mdk in ds.attrs:
            ds.attrs[mdk]=mdv
    for mdk,mdv in desc["metadata"].items():
        if not mdk in ds.attrs:
            ds.attrs[mdk]=mdv
    return mapper_dict, ds

def add_nextgems(mapper_dict, dsdict):
    caturl=test_catalog()
    if not caturl:
        return mapper_dict, dsdict
    ngccat=intake.open_catalog(caturl)
    prodcat=intake.open_catalog("https://www.wdc-climate.de/ui/cerarest/addinfoDownload/nextGEMS_prod_addinfov1/nextGEMS_prod.yaml")
    md=yaml.safe_load(prodcat.yaml())["sources"]["nextGEMS_prod"]["metadata"]
    for ia in DS_ADD:
        desc=ngccat[ia].describe()
#        if type(desc["args"]["urlpath"])==str and desc["args"]["urlpath"].endswith("zarr"):
        if "user_parameters" in desc:
            ups=desc["user_parameters"]
            uplists=[up["allowed"] for up in ups]
            combinations = list(
                    itertools.product(
                        *uplists
                        )
                    )
            combdict=[
                    {
                        ups[i]["name"]:comb[i]
                        for i in range(len(ups))
                        }
                    for comb in combinations
                    ]
            storage_options=desc["args"].get("storage_options",{})
            if type(desc["args"]["urlpath"]) == str :
                if desc["args"]["urlpath"].startswith("reference") and not storage_options.get("remote_protocol",None):
                    storage_options=dict(lazy=True,remote_protocol="file")
            for comb in combdict:
                iakey='nextgems.'+ia+"."+'_'.join([str(a) for a in list(comb.values())])
                try:
                    dsdict[iakey]=ngccat[ia](**comb,chunks="auto",storage_options=storage_options).to_dask()
                    mapper_dict, dsdict[iakey]=refine_nextgems(mapper_dict, iakey,dsdict[iakey],md,desc)
                except Exception as e:
                    print(e)
                    pass
        else:
            iakey='nextgems.'+'.'.join(ia.split('.')[1:])
            try:
                dsdict[iakey]=ngccat[ia](chunks="auto").to_dask()
                mapper_dict, dsdict[iakey]=refine_nextgems(mapper_dict,iakey,dsdict[iakey],md,desc)
            except:
                print(iakey+" does not work")
                pass
    iakey="nextgems.IFS_2.8-FESOM_5-production.2D_hourly_025deg"
    dsdict[iakey]=xr.open_dataset(
            "reference::/"+
            "/work/bm1344/DKRZ/kerchunks_batched/NGC-IFS-tco3999-ng5-production/2D_hourly_0.25deg.parq",
            engine="zarr",
            storage_options=dict(lazy=True,remote_protocol="file"),
            consolidated=False,
            chunks="auto"
            )
    desc=dict(args=dict(storage_options=dict(lazy=True,remote_protocol="file")),metadata={})
    mapper_dict, dsdict[iakey]=refine_nextgems(mapper_dict,iakey,dsdict[iakey],md,desc)
    #
    print(dsdict.keys())
    return mapper_dict, dsdict
