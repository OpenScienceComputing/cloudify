from cloudify.utils.datasethelper import *
import intake
from copy import deepcopy as copy


def add_dyamond(mapper_dict, dsdict):
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_hk25/catalog.yaml"
    DS_ADD = ["EU.icon_d3hp003"]
    cat = intake.open_catalog(source_catalog)
    localdsdict = get_dataset_dict_from_intake(
        cat,
        DS_ADD,
        prefix="dyamond",
        storage_chunk_patterns=["EU"],
        allowed_ups=dict(zoom=["10","11"]),
        mdupdate=True
    )
    
    for dsn, ds in localdsdict.items():
        mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsn, ds)
        ds = adapt_for_zarr_plugin_and_stac(dsn, ds)
        ds = set_compression(ds)
        dsdict[dsn]=ds
    
    del localdsdict

    return mapper_dict, dsdict
