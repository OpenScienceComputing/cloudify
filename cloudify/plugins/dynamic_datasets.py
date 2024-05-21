from xpublish import Plugin, hookimpl
import intake
import pickle
CATALOG_FILE="/work/bm1344/DKRZ/intake_catalogues/dkrz/disk/main.yaml"
PICKLE_TRUNK="/work/bm1344/DKRZ/pickle/"
from fastapi import HTTPException

class DynamicKerchunk(Plugin):
    name : str = "dynamic-kerchunk"
       
    @hookimpl
    def get_datasets(self):
        global dsdict
        return list(dsdict.keys())

    @hookimpl
    def get_dataset(self, dataset_id: str):
        global dsdict
        if dataset_id not in dsdict:
            raise HTTPException(status_code=404, detail=f"Could not find {dataset_id}")
        return dsdict[dataset_id]


#    @hookimpl
    def get_datasets2(self):
        cat = intake.open_catalog(CATALOG_FILE)
        dsets = list(cat)
        pickle_abs = list_pickles(PICKLE_TRUNK)
        pickle_list = ['_'.join(a.split('/')[-2:]) for a in pickle_abs]
        pickle_list = ['.'.join(a.split('.')[:-1]) for a in pickle_list]
        dsets+=pickle_list
        dsetsnew = [a+"_12bit" for a in dsets if not "remap" in a and not "025" in a]
        dsets_compr = [a for a in dsets if a+"_12bit" not in dsetsnew]
        dsetsnew+=dsets_compr
        dsetsnew.sort()
        return dsetsnew

#    @hookimpl
    def get_dataset2(self, dataset_id: str):
        lcompress = False
        if dataset_id.endswith("_12bit"):
            lcompress = True
            dataset_id = dataset_id.replace("_12bit","")
            
        cat = intake.open_catalog(CATALOG_FILE)
        dsets = list(cat)
        pickle_abs = list_pickles(PICKLE_TRUNK)
        pickle_list = ['_'.join(a.split('/')[-2:]) for a in pickle_abs]
        pickle_list = ['.'.join(a.split('.')[:-1]) for a in pickle_list]
        if dataset_id in dsets or any([dataset_id in a for a in pickle_list]):
            if dataset_id in dsets:
                ds = cat[dataset_id].to_dask()
            else:
                fn=pickle_abs[pickle_list.index(dataset_id)]
                ds = pickle.load(
                        fs.open(
                            fn
                            ).open()
                        )
                #if "fesom" in dsid
                #apply_ufunc
                #set encoding

            return ds.squeeze(drop=True)

        return None
