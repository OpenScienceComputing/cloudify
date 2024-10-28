import pandas as pd
from tqdm import tqdm
from mytoken import *
import json
import os
import xarray as xr
import panel as pn
pn.extension("tabulator")

os.environ["OS_STORAGE_URL"]=OS_STORAGE_URL
os.environ["OS_AUTH_TOKEN"]=OS_AUTH_TOKEN
import fsspec
fs=fsspec.filesystem("swift")
swift_account_url=OS_STORAGE_URL.replace("https://","swift://").replace("/v1/","/")
plotdir=swift_account_url+"/plots"
#fs.makedir(plotdir) tobedone in browser
cloudmapper=fs.get_mapper(plotdir)
plots=list(filter(lambda a: "tabu.htm" not in a and "test.htm" not in a, list(cloudmapper.keys())))

DRS=["activity_id","domain_id","institution_id","driving_source_id","driving_experiment_id",
        "driving_variant_label","source_id","version_realisation","frequency","variable_id","version","plotname"]
FN=["hvplot","variable_id","frequency","kind","color","times"]
using=["plotname","frequency","variable_id","kind","color","times"]

from bokeh.models import HTMLTemplateFormatter

bokeh_formatters = {
    "url": HTMLTemplateFormatter(template="<code><%= value %></code>")
}

def map_plot(entry):
    resdict={}
    for part,value in zip(DRS,entry.split('/')):
        if any(part in a for a in using):
            resdict[part]=value
    resdict["url"]=(plotdir+"/"+entry).replace("swift://","https://").replace("dkrz.de/","dkrz.de/v1/")
    for part,value in zip(FN,resdict["plotname"].split('_')):
        if any(part in a for a in using):
            resdict[part]=value
    del resdict["plotname"]
    return resdict   

def make_clickable(val):
    return '<a target="_blank" href="{}">{}</a>'.format(val, val)

df=pd.DataFrame(list(map(map_plot,plots)))
df["url"]=df["url"].apply(make_clickable)

tabu = pn.widgets.Tabulator(
    df,
    show_index=False,
    header_filters=True,
    selectable=1,
    widths=dict(url=200),
    pagination="local",
    formatters=bokeh_formatters
)
tabu.save("tabu.html")
