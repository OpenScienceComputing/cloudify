from tqdm import tqdm
from mytoken import *
import json
import os
import xarray as xr

#https://swift.dkrz.de/v1/dkrz_475f8922-045d-43f4-b661-0b053074407e/test/no_ave.txt
os.environ["OS_STORAGE_URL"]=OS_STORAGE_URL
os.environ["OS_AUTH_TOKEN"]=OS_AUTH_TOKEN

import fsspec
fh=fsspec.filesystem("http",timeout=9999999)
fs=fsspec.filesystem("swift")
swift_account_url=OS_STORAGE_URL.replace("https://","swift://").replace("/v1/","/")
plotdir=swift_account_url+"/plots"
print(plotdir)
#fs.makedir(plotdir) tobedone in browser
cloudmapper=fs.get_mapper(plotdir)

kinds=["quadmesh","contourf"]
color="coolwarm"
times=["-1_0","-12_0"]

host=os.environ["HOSTNAME"]
baseurl="http://"+host+":9000/datasets"
with fh.open(baseurl,"rb") as fp:
    datasets=fp.read()

cloudmapper["test.htm"]=datasets
datasets=eval(datasets)
print(datasets)
chosen=0
#CORDEX.CMIP6.DD.EUR-12.CLMcom-DWD.ERA5.evaluation.r1i1p1f1.ICON-CLM-202407-1-1.v0-r0.1hr.v20240713
hosted='.'.join(datasets[chosen].split('.')[2:11])
hosted=datasets[chosen]
version=datasets[chosen].split('.')[11]
print(hosted)
print(version)
zarrurl='/'.join([baseurl,hosted])+"/zarr"
ds=xr.open_zarr(
        zarrurl,
        consolidated=True,
        decode_cf=True
        )
hostedtrunk='/'.join(hosted.split('.')[2:-1])
for dv in tqdm(ds.data_vars):
    if "bnds" in dv or "bounds" in dv or "rotated_lat" in dv or "vertices" in dv:
        continue
    print(dv)
    for kind,time in zip(kinds,times):
        plotname='_'.join(["hvplot",dv,hosted.split('.')[-2],kind,color,time])+".htm"
        trunk='/'.join([hostedtrunk,dv,version])
        entry='/'.join([trunk,plotname])
        url='/'.join([baseurl,hosted,"plot",dv,kind,color,"lat/20_73/lon/-45_65/time",time])
        print(entry+": "+url)
        if entry in cloudmapper:
            continue
        try:
            cloudmapper[entry]=fh.cat(url)
        except:
            print("Did not work for "+entry)
            continue
