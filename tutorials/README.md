# Cloudify Workshop

Building competence for the eerie.cloud data workflow.

Links:
- [Gitlab Repo](https://gitlab.dkrz.de/data-infrastructure-services/cloudify)
- [Technical overview paper](https://pad.gwdg.de/OZo5HMC4R6iljvZHlo-BzQ?view#)
- Access and usage:
    - [Slideshow](https://pad.gwdg.de/HD0fHG-STCGzCqBtZCx8iQ?view#)
    - [Notebook](https://easy.gems.dkrz.de/simulations/EERIE/eerie_data-access_online.html)
- Ingest: [Lake house approach paper](https://pad.gwdg.de/gzXeJB85QTC6LNNlhA1A5A?both#)

For the cloudify tarining on levante, start a jupyterhub server on either a compute (recommended) or an interactive node.

## [Concepts](https://docs.google.com/presentation/d/1OrPWOZXAs0rRRdfomPABo1-rSaHyH8amlfLOIvRd48A/edit?usp=sharing)

- Zarr - a (not only) cloud-optimized data format for ESM output
- Benefits of cloud storages and why we not fully use it (yet)
- Xpublish - the cloud data emulator with server-side processing

## [Cloudify on Levante](https://gitlab.dkrz.de/data-infrastructure-services/cloudify/-/tree/main/workshop?ref_type=heads)

- How to start an app
- The various ways to access cloudified data through catalogs, xarray and cdo
- Use-cases and preparations for a data server on the PB scale
    - Server-side processing for lossy compression, rechunking and on-the-fly post-processing
    - Large aggregations: Zarr becomes the *catalog* with kerchunked input and the kerchunk API
    - SPOA for ingestions

## [The eerie.cloud implementation](https://docs.google.com/presentation/d/1L7ehzS5O2n9O131MZdk47SIQflhuyMedJ4rwO5P3NvM/edit?usp=sharing)

### Data preparation for ingestion

- Requirements for a performant data server
- Kerchunking: we create virtual datasets by extracting the storage chunks of netcdf and grib files, concat them and store the consolidated dataset in a lazy format based on parquet tables.
- Catalogs: The virtual zarr datasets are collected in an intake catalog based on intake-xarray. This catalog is used for eerie.cloud ingestion.

### Server setup

- The openstack VM setting, Nginx and a xpublish plugin.

### STAC API

- The catalog infrastructure based on a mixture of static and dynamic STAC catalogs.

## User guidance

Live show

**Navigation** through eerie.cloud with static stac catalogs in the web-browser using the stac-browser.

**User guide** with the [easy gems notebook](https://easy.gems.dkrz.de/simulations/EERIE/eerie_data-access_online.html)

**Applications**: Jupyterlite, Gridlook and a WPS based on a simple xarray API

**Integrations** to be discussed:

- Freva
- Warmworld approach

## Past events

8.1. 13:00-15:00
hybrid: room #23 or https://eu02web.zoom-x.de/j/9290696892?pwd=WElNS0xIMGp3ZERIRTlYdjR0U3ZaUT09 

For DKRZ DM.