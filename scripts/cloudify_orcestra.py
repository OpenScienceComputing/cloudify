import glob
import datetime
from tqdm import tqdm
import xarray as xr
from datasethelper import *

TRUNK = "/work/mh0492/m301067/orcestra/healpix/"
DIMS = ["2d", "3d"]
conf_dict = dict(
    source_id="ICON-LAM",
    institution_id="MPI-M",
    project_id="ORCESTRA",
    activity_id="ORCESTRA",
    experiment_id="orcestra_1250m",
    authors="Romain Fiévet",
    contact="romain.fievetATmpimet.mpg.de",
    description="https://orcestra-campaign.org/lam.html",
)


def add_orcestra(mapper_dict, dsdict):
    init_dates_trunks = [
        a for a in sorted(glob.glob(TRUNK + "/*")) if a.split("/")[-1][0] == "0"
    ]
    dsone = None
    for ini in tqdm(init_dates_trunks):
        init_date = ini.split("/")[-1]
        for dim in DIMS:
            dstrunk = f'{ini}/{conf_dict["experiment_id"]}_{init_date}_{dim}_hpz12.zarr'
            dsname = f'{conf_dict["project_id"].lower()}.{conf_dict["source_id"]}.s2024-{init_date[0:2]}-{init_date[2:4]}_{dim}_PT10M_12'
            if dim == "3d":
                dsname = dsname.replace("PT10M", "PT4H")
            ds = xr.open_dataset(
                dstrunk, engine="zarr", consolidated=True, chunks="auto"
            )
            if not dsone:
                dsone = ds.copy()
            ds["cell"] = dsone["cell"]
            ds.attrs.update(conf_dict)
            mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsname, ds)
            ds = adapt_for_zarr_plugin_and_stac(dsname, ds)
            ds = set_compression(ds)
            dsdict[dsname] = ds
    return mapper_dict, dsdict
