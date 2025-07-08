defaults=dict(
    EERIE_CLOUD_URL="https://eerie.cloud.dkrz.de/datasets",
    STAC_API_URL="http://stac2.cloud.dkrz.de:8081/fastapi",
    STAC_API_URL_EXT="https://stac2.cloud.dkrz.de/fastapi",
    STAC_COLLECTION_ID_TEMPLATE="<project_id>-<activity_id>-<institution_id>-<source_id>-<experiment_id>-<version_id>",
    STAC_ITEM_ID_XPUBLISH_PREFIX="xp",
    STAC_ITEM_XPUBLISH_TITLE_SUFFIX=" kerchunked",
    PROVIDER_DKRZ = dict(
        name="DKRZ",
        description="The data host of the items and assets",
        roles=["host"],
        url="https://dkrz.de",
    ),
    COLLECTION_SNIPPET="""\n You can use this collection with [pystac-client](https://pystac-client.readthedocs.io/en/stable/) and [xarray](https://github.com/pydata/xarray) :\n 
    ``` 
    import xarray as xr
    from pystac_client import Client as psc
    from tqdm import tqdm
    dsdict = {}
    client = psc.open(
        "STAC_API_URL"
    )
    col=client.get_collection("REPLACE_COLID")
    items = list(col.get_all_items())
    asset_name = "dkrz-disk"
    for item in tqdm(items):
        dsdict[item.id] = xr.open_dataset(
            item.assets[asset_name].href,
            **item.assets["dkrz-disk"].extra_fields["xarray:open_kwargs"],
            storage_options=item.assets["dkrz-disk"].extra_fields.get("xarray:storage_options")
        )
    ``` """,
    ITEM_SNIPPET="""\nYou can use this dataset in xarray.\n 
    ``` 
    import xarray as xr
    import requests
    item=requests.get("REPLACE_ITEMURI").json()
    asset="dkrz-disk"
    #asset="eerie-cloud" # from everywhere else
    xr.open_dataset(
        item["assets"][asset]["href"],
        **item["assets"][asset]["xarray:open_kwargs"],
        storage_options=item["assets"][asset].get("xarray:storage_options")
    )
    ``` """        
)
