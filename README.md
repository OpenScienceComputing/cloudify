# Cloudify

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)](https://hub.docker.com/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

[cloudify](https://gitlab.dkrz.de/data-infrastructure-services/cloudify) is a modular application designed to serve Earth System Model (ESM) simulation output as cloud-optimized datasets. Built on a customized version of the [xpublish](https://github.com/xpublish-community/xpublish) framework and extended with plugins, cloudify exposes data as [zarr](https://github.com/zarr-developers/zarr-python) endpoints through a RESTful API powered by [FastAPI](https://github.com/tiangolo/fastapi). By supporting multiple storage backends and virtual zarr datasets, cloudify enables efficient, scalable access to data from heterogeneous sources via a unified interface. This approach enhances the FAIRness (Findability, Accessibility, Interoperability, and Reusability) of ESM output and closely mimics the behavior of truly cloud-native datasets (Abernathey, 2021). 

- [Technical description](https://pad.gwdg.de/MmzEY9KEQcOe6y8er62EZw?both)

## Installation

- [Docker](Dockerfile)
- [Conda+pip](install.sh)

## Usage

We differentiate between data providers and data consumers.

### Provisioning

- [Python examples](scripts)
    - [Dynamic datasets](scripts/host_dynamic.py)
    - [Eerie-cloud like](scripts/cataloghost.py)
- [Workshop](workshop)

#### With Dask to apply functions before hosting:

With dask support, Data as a service (DaaS) is enabled. It has to be adjusted with fastapi's async Threadpool.

1. Open xarray datasets within the *main* function. This avoids infinite subprocess creation.
1. Create a dask cluster with sufficient resources. You can use the `get_dask_cluster` function from `cloudify.utils.daskhelper`.
1. Set the environment variable ZARR_ADDRESS to the scheduler address. This will be used by the `/zarr`-API to calculated chunks.

**Minimal Example**

```python
from cloudify.utils.daskhelper import get_dask_cluster
import asyncio
import os
import xpublish as xp
import xarray as xr
import nest_asyncio
nest_asyncio.apply()

if __name__ == "__main__":  # 
    import dask
    zarrcluster = asyncio.get_event_loop().run_until_complete(get_dask_cluster())
    os.environ["ZARR_ADDRESS"] = zarrcluster.scheduler._address
    dsdict={}
    dsdict["test"] = xr.open_dataset(DATASET,chunks="auto")
    collection = xp.Rest(dsdict)
    collection.serve(host="0.0.0.0", port=9000)
```

### Consumption

All endpoints can be listed programmatically with python:

```python
import requests
fast=requests.get(SERVER_URL+"/openapi.json").json()
print(list(fast["paths"].keys()))
```

#### Metadata info per xarray dataset

Default xpublish dataset plugin endpoints:

```python
 '/datasets',
 '/datasets/{dataset_id}/',
 '/datasets/{dataset_id}/keys',
 '/datasets/{dataset_id}/dict',
 '/datasets/{dataset_id}/info'
```

#### Access

**Zarr Raw**. [Self developed](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints:

```python
import xarray as xr
xr.open_dataset(
    f"{SERVER_URL}/datasets/{dataset_id}/kerchunk",
    engine="kerchunk",
    chunks="auto"
) 
```

**Zarr Dask backed**. Default plugin.  Endpoints:

```python
import xarray as xr
xr.open_dataset(
    f"{SERVER_URL}/datasets/{dataset_id}/zarr",
    engine="zarr",
    chunks="auto"
) 
```

#### Catalogs

**STAC**

Self defined plugin. Endpoints:

```python
import pystac
stac_collection = pystac.Collection.from_file(
    f"{SERVER_URL}/stac-collection-all.json"
)
items = list(stac_collection.get_all_items())
```

**Intake**

Intake-xpublish plugin.

```python
import intake
dataset_id="test"
api="kerchunk"
xpublish_intake_uri=SERVER_URL+"/intake.yaml"
cat=intake.open_catalog(xpublish_intake_uri)
#xarray dataset:
ds=cat[dataset_id](method=api).to_dask()
```

Server-side processing

**Plotting**

[Self-developed plugin](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints.

```python
import requests
plot=requests.get(f"{SERVER_URL}/datasets/{dataset_id}/plot/{var_name}/{kind}/{cmap}/{selection}").content
```

**Diagnostics**

[Self-developed plugin](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints:

```python
import requests
data_var_view=requests.get(f"{SERVER_URL}/datasets/{dataset_id}/groupby/{variable_name}/{groupby_coord}/{groupby_action}")
```
