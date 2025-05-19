from datasethelper import *
import intake
from copy import deepcopy as copy


def add_dyamond(mapper_dict, dsdict):
    source_catalog = "/work/bm1344/DKRZ/intake_catalogues_hk25/catalog.yaml"
    DS_ADD = ["EU.icon_d3hp003"]
    cat = intake.open_catalog(source_catalog)
    for dsn in DS_ADD:
        iakey = dsn
        desc = cat[dsn].describe()
        args = copy(desc["args"])
        del args["urlpath"]
        args["chunks"] = "auto"
        ups = desc.get("user_parameters")
        md = desc.get("metadata")
        if ups:
            for upid, up in enumerate(ups):
                if up["name"] == "zoom":
                    ups[upid]["allowed"] = ["10", "11"]

            combination_list = get_combination_list(ups)
            args["chunks"] = {}
            for comb in combination_list:
                iakey = (
                    "dyamond."
                    + dsn
                    + "."
                    + "_".join([str(a) for a in list(comb.values())])
                )
                #                try:
                if True:
                    ds = cat[dsn](**args, **comb).to_dask()
                    if len(ds.data_vars) == 0:
                        continue
                    mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, iakey, ds)
                    if md:
                        ds.attrs.update(md)
                    ds = adapt_for_zarr_plugin_and_stac(iakey, ds)
                    ds = set_compression(ds)
                    dsdict[iakey] = ds
        #                except:
        #                    print("Could not open "+iakey)
        #                    continue
        else:
            try:
                ds = cat[dsn](**args).to_dask()
                mapper_dict, ds = reset_encoding_get_mapper(mapper_dict, dsn, ds)
                ds = adapt_for_zarr_plugin_and_stac(dsn, ds)
                ds = set_compression(ds)
                dsdict[iakey] = ds
            except:
                print("Could not open " + iakey)
                continue
    return mapper_dict, dsdict
