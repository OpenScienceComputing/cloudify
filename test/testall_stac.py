import socket
import requests
import pystac as psc
import xarray as xr
from tqdm import tqdm
import json
import fsspec

hostname = socket.gethostname()
port = 9000
hosturl=f"http://{hostname}:{port}"
stacurl='/'.join([hosturl,"stac-collection-all.json"])
col=psc.Collection.from_file(stacurl)
print(stacurl)
child_links = col.get_child_links()
asset_name="dkrz-disk"
for link in tqdm(child_links):
#    try:
    if True:
        item=psc.Item.from_file(link.href)
        json.dump(
                item.to_dict(),
                fsspec.open(
                    item.id+".json",
                    "w"
                    ).open(),
                )
#        xr.open_dataset(
#            item.assets[asset_name].href,
#            **item.assets["dkrz-disk"].extra_fields["xarray:open_kwargs"],
#            storage_options=item.assets["dkrz-disk"].extra_fields.get("xarray:storage_options")
#        )
#    except:
#        print(link)
