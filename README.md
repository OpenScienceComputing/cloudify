# Cloudify

- [Technical description](https://pad.gwdg.de/MmzEY9KEQcOe6y8er62EZw?both)

This repo contains code used for the data server 'km-scale-cloud.dkrz.de' for high resolution Earth System Model simulation output. 
As a pilot, we host data from experiments produced within the EU project EERIE and stored at the German Climate Computing Center (DKRZ).

The 'km-scale-cloud' makes use of the python package [xpublish](https://github.com/xpublish-community/xpublish).
Xpublish is a plugin for xarray (Hoyer, 2023) which is widely used in the Earth System Science community.
It serves ESM output formatted as zarr (Miles, 2020) via a RestAPI based on FastAPI.
Served in this way, the data imitates cloud-native data and features many capabilities of cloud-optimized data.
We explain a workflow implemented at DKRZ which allows to host volumes on the petabyte scale
as well as chunk-based access to both raw and on-the-fly, server-side post-processed data.

## Motivation

Earth System Model (ESM) output can be provided openly and efficiently by storing it in cloud-storages in cloud-optimized formats. 
This allows for chunk-based, parallel access via standard protocols like s3 and http to an easily scalable storage resource. 
However, it can be costful to bring data there: 
First, one needs to convert data from a source format to a target format which takes efforts by a human. 
Projects like [pangeo-forge](https://pangeo-forge.readthedocs.io/en/latest/) may simplify this conversion 
but are not adapted to institutional cloud storage providers. 
Secondly, the source data often remains on the source storage resulting in data duplication, 
e.g. if the source data is on a high-performance storage whose properties are needed for further processing. 

State-of-the-art ESM simulations are highly resolved in both temporal and spatial dimensions 
resulting in large volumes of model output (O(PB)). 
Working with this output from remote is highly limited by band-width: 
E.g., DKRZ only allows for 30GiB/s total uplink that all remote users have to share. 
This demands for strong data reduction methods including lossy compressions when data is transferred to clients. 

At DKRZ, we established a server prototype named **"km-scale-cloud"** 
to provide efficient data access and offer server-side resources for the global community 
with the goal to reduce the amount of data to be transmitted. 
We leaverage the pangeo software stack to design a workflow based on python tools that are well known and widely used
so that the server solution may be well accepted, understood and commonly extended.

We name this workflow ''*to **cloudify** the ESM data*'': 
Data is served by the python package xpublish in a way that it mimicks data in the cloud 
as if it were stored as cloud-optimized zarr data. 
Such data provision inherits the benefits of the zarr format for users, 
but in addition can be modified and processed on server-side before transfer by applying xarray. 
A key feature of *cloudifying* is that no data conversion and transfer to cloud is necesesary, 
so that it is faster and cheaper in comparison to a data move.

## Usage of the km-scale-cloud

To gain overview of the full content of the data server 
from a single point of access, we added catalog endpoints which collect the zarr data endpoints of both APIs. 
We support both catalog tools [intake v1](https://github.com/intake/intake)
(based on [intake-xarray](https://github.com/intake/intake-xarray)) and [stac](https://github.com/radiantearth/stac-spec) 
as they are widely used in the ESM community. 
The recommended data workflow for clients is depicted in Fig. X.

![](https://pad.gwdg.de/uploads/11823bbd-2c7f-450d-938f-1cf00890dc2b.png)

***Fig. X:*** A sketch of the recommended user data workflow for accessing cloudified data through the "eerie.cloud". 
First, the user opens a catalog, either intake or stac, where all served datasets are included. 
Second, a served zarr dataset is opened via this catalog. 
This can either be through the *raw API* or the *dask API*. 
Third, chunks are retrieved via fsspec from the underlying data lake.

**Intake**

First, we made use of the [xpublish-intake plugin](https://github.com/xpublish-community/xpublish-intake/) 
to automatically create an intake catalog for all datasets available on the xpublish server. 
This enables programmatic access:

```python
import intake
xpublish_intake_url="https://eerie.cloud.dkrz.de/intake.yaml"
cat=intake.open_catalog(xpublish_intake_url)
```

**Stac**

Intake does not provide a pretty user display of catalogs.
We found that [stac] catalogs can fill this gap so that we developped a stac-plugin.
Any stac catalogs can be prettily displayed and rendered in a web browser using the [stac-browser](github.com/radiantearth/stac-browser).

For each dataset, a [tac item is served that includes information on temporal and spatial coverage 
based on the dataset's coordinates. 
Title and description are generated from on attributes of the dataset. 
A code snippet for using the item, both on DKRZ's HPC system as well as from everywhere, is also presented in markdown.

Each item includes links to the two zarr data APIs, 
the dataset HTML view API as well as other apps that make use of the data, as assets. 

![](https://pad.gwdg.de/uploads/8b699ca8-95ab-4218-b765-c2c9f0c2d3e5.png)
***Fig. X***: An example stac-item view within the stac-browser.

All items are collected in a overall collection which is served via the
[stac-collection-all.json](eerie.cloud.dkrz.de/stac-collection-all.json) endpoint. 
This however does not allow efficient browsing. 
We therefore build a overall [stac catalog](https://swift.dkrz.de/v1/dkrz_7fa6baba-db43-4d12-a295-8e3ebb1a01ed/catalogs/stac-catalog-eeriecloud.json) for the set-up eerie.cloud which not only points to the *all-items collection* , but furthermore to more structured, hierarchical subcollections. One example is the [EERIE dataset collection](https://raw.githubusercontent.com/eerie-project/intake_catalogues/refs/heads/main/dkrz/disk/stac-templates/catalog-experiments.json) 
which is itself a collection tree. It contains collections for model-experiment combinations. 
These collections again contain corresponding items from eerie.cloud. Served in this way, EERIE items become easily browsable. 
If clients call the root route of the eerie.cloud, they are redirected to a stac-browser's view of the main catalog.

Cloudified data enables efficient and high granular data access 
that allows the community to develop in-browser data tools, e.g. typescript based. 
This comprises monitoring and visualisation, but also small analysis use cases.

## Endpoints and Xpublish-plugins

All endpoints can be listed programmatically with python:

```
import json
import fsspec
fast=json.load(fsspec.open("https://eerie.cloud.dkrz.de/openapi.json").open())
list(fast["paths"].keys())
```

### Metadata info per xarray dataset

Default xpublish dataset plugin. Endpoints:

```
 '/datasets',
 '/datasets/{dataset_id}/',
 '/datasets/{dataset_id}/keys',
 '/datasets/{dataset_id}/dict',
 '/datasets/{dataset_id}/info'
```

### Access


**Zarr Raw**. [Self developed](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints:

```
 '/datasets/{dataset_id}/kerchunk/{key}',
```

**Opendap**. Xpublish-opendap plugin. Endpoints

```
 '/datasets/{dataset_id}/opendap.dds',
 '/datasets/{dataset_id}/opendap.das',
 '/datasets/{dataset_id}/opendap.dods',
```

**Zarr Dask backed**. Default plugin.  Endpoints:

```
 '/datasets/{dataset_id}/zarr/.zmetadata',
 '/datasets/{dataset_id}/zarr/.zgroup',
 '/datasets/{dataset_id}/zarr/.zattrs',
 '/datasets/{dataset_id}/zarr/{var}/{chunk}',
```

### Catalogs

**STAC**

Self defined plugin. Endpoints:

``` 
'/stac-collection-eerie.json',
'/datasets/{dataset_id}/stac',
```

**Intake**

Intake-xpublish plugin. Endpoints:

```
 '/intake.yaml',
 '/datasets/{dataset_id}/catalog.yaml',
```

Server-side processing

**Plotting**

[Self-developed plugin](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints:

```
 '/datasets/{dataset_id}/plot/{var_name}/{kind}/{cmap}/{selection}'
```

**Diagnostics**

[Self-developed plugin](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/cloudify/plugins?ref_type=heads). Endpoints:

```
 '/datasets/{dataset_id}/groupby/{variable_name}/{groupby_coord}/{groupby_action}',
