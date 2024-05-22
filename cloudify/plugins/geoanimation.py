import os
from cloudify.utils.daskhelper import *
import io
import xarray as xr
from xpublish import Plugin, hookimpl, Dependencies
from xpublish.dependencies import get_dataset
from typing import Sequence
from copy import deepcopy as copy
import asyncio
import hvplot.xarray
import nest_asyncio

nest_asyncio.apply()
import cartopy.crs as ccrs
from fastapi import FastAPI, APIRouter
from fastapi import HTTPException, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from starlette.responses import StreamingResponse
import holoviews as hv
import panel as pn

# from bokeh.embed import server_document
import numpy as np

# Mount a directory as a static route on the file router


class PlotPlugin(Plugin):
    name: str = "PlotPlugin"

    dataset_router_prefix: str = "/plot"
    dataset_router_tags: Sequence[str] = ["plot"]

    hvplotopts: dict = dict(
        use_dask=True,
        #                width=1920,
        #                fontscale=2.6,
        fontscale=1.5,
        min_width=1280,
        max_width=1920,
        max_height=1080,
        min_height=720,
        responsive=True,
        x="lon",
        coastline="50m",
        y="lat",
        levels=20,
        widget_type="scrubber",
        widget_location="bottom",
        features=["borders"],
        legend=False,
        rasterize=False,
        #                crs=ccrs.PlateCarree(),
        #                crs=ccrs.Mercator(),
    )

    def drop_noplot_vars(self, ds, coords):
        for dropvar in ["cell_sea_land_mask", "coast", "bnds"]:
            if dropvar in ds.coords or dropvar in ds.dims:
                ds = ds.drop(dropvar)
        keeplist = ["lat", "lon", "time", "i", "j"]
        if coords:
            keeplist += list(coords.keys())
        droppable = list(set(ds.coords) - set(keeplist))
        if len(droppable) > 0:
            for s in droppable:
                if s not in ds.dims:
                    ds = ds.drop(s)
                else:
                    print("isel " + s)
                    ds = ds.isel({s: 0})
        return ds

    def change_units(self, ds):
        if (
            "lat" in ds.coords
            and "lat" not in ds.dims
            and not ds["lat"].attrs["units"].startswith("degrees")
        ):
            print("Applying rad2deg")
            for c in ["lat", "lon"]:
                ds[c] = xr.apply_ufunc(np.rad2deg, ds[c], dask="parallelized")
                ds[c].attrs["units"] = "degrees"
        return ds

    def prepare_coords(self, ds, var_name, coords):
        coords_to_load = None

        if not coords:
            #            if not "time" in ds[var_name].dims:
            #                del ds
            #                ds=None
            #            else:
            if "time" in ds[var_name].dims:
                ds = ds.isel({"time": range(-12, -1)})
        else:
            if "time" in coords.keys():
                ds = ds.isel(time=range(coords["time"][0], coords["time"][1]))
                del coords["time"]
            elif "time" in ds[var_name].dims:
                ds = ds.isel({"time": range(-12, -1)})
            coords_to_load = [a for a in coords.keys() if ds[a].chunks != None]
        return ds, coords_to_load

    def unstacke_and_nninterpol(self, ds):
        if "value" in ds.dims and "lat" not in ds.dims:
            print("Try unstack")
            status = "1."
            try:
                unstack = (
                    ds.rename({"value": "latlon"})
                    .set_index(latlon=("lat", "lon"))
                    .unstack("latlon")
                )
                status = "unstack"
                # 2. Get all longitudes from the equator because this is the max and we can use it for the target grid:
                subsel = unstack
                for d in unstack.dims:
                    if d != "lon" and d != "lat":
                        subsel = subsel.isel({d: 0})
                subsel = unstack.sel(lat=0, method="nearest")
                target_lons = subsel.dropna(dim="lon")["lon"]
                print(target_lons)
                status = "target_lons"
                interp = unstack.interpolate_na(
                    dim="lon", method="linear", period=360.0
                )
                status = "interp"
                reindexed = interp.reindex(lon=target_lons)
                ds = reindexed
                return ds
            except Exception as e:
                print(e)
                print("Failed at " + status)
        return ds

    async def prepare_ds(self, dataset, var_name, coords):
        ds = dataset.copy(deep=True)
        client = await get_dask_client()
        if var_name not in ds.variables:
            await client.close()
            raise HTTPException(
                status_code=404, detail=f"Variable '{var_name}' not found in dataset"
            )
        ds = ds[[var_name]]
        ds = self.drop_noplot_vars(ds, coords)
        ds = self.change_units(ds)
        ds, coords_to_load = self.prepare_coords(ds, var_name, coords)
        # if not ds:
        #    await client.close()
        #    raise HTTPException(status_code=404, detail=f"Could not subset. Please specify range.")
        if coords_to_load:
            dscoords = ds[coords_to_load]  # .reset_coords()[coords_to_load]
            dscoords = client.persist(dscoords)
            dscoords = await client.compute(dscoords)
            for a in coords_to_load:
                ds[a] = dscoords[a]
        lonmax = ds["lon"].max()
        #        lonmax = await client.compute(dsmax)
        if lonmax > 180.0:
            ds["lon"] = xr.where(ds["lon"] > 180.0, ds["lon"] - 360.0, ds["lon"])
            ds = ds.sortby("lon")
        if coords:
            for coord, rangeval in coords.items():
                if coord != "time":
                    ds = ds.where(
                        ((ds[coord] >= rangeval[0]) & (ds[coord] < rangeval[1])),
                        drop=True,
                    )
        print("size")
        print(ds.nbytes)
        if ds[var_name].size == 0:
            await client.close()
            raise HTTPException(
                status_code=404, detail=f"Your subset leads to empty array."
            )

        #        droppable=list(set(ds.squeeze().dims)-set(["time","lat","lon","ncells","ncells2"])-set(coords.keys()))#
        #                ds=ds.drop(droppable)
        #        ds=ds.squeeze()
        if ds.nbytes > 5 * 1024 * 1024 * 1024:
            await client.close()
            raise HTTPException(
                status_code=404, detail="Dataset too large after subsetting"
            )
        ds = client.persist(ds[var_name])
        ds = await client.compute(ds)
        ds = self.unstacke_and_nninterpol(ds)
        mima = [ds.min(), ds.max()]
        #        mima = [await client.compute(ds.min()),await client.compute(ds.max())]
        cmin, cmax = round(mima[0].data.item(), 6) - 10**-6, round(
            mima[1].data.item(), 6
        )
        await client.close()
        return ds, (cmin, cmax)

    def get_titles(self, dataset, var_name):
        parts = dataset.attrs["_xpublish_id"].split(".")
        title = "Data from "
        title += " ".join(parts[:-1])
        title += "\n for aggregation "
        title += parts[-1]
        title += "\n"
        name = var_name
        if "name" in dataset[var_name].attrs:
            name = dataset[var_name].attrs["name"]
        if "long_name" in dataset[var_name].attrs:
            name = dataset[var_name].attrs["long_name"]
        clabel = name
        if "units" in dataset[var_name].attrs:
            clabel += " [" + dataset[var_name].attrs["units"] + "]"
        return title, clabel

    async def gen_plot(self, dataset, var_name, coords=None, kind="image", cmap="jet"):
        from dask.distributed import Client

        zarraddress = os.environ["ZARR_ADDRESS"]
        title, clabel = self.get_titles(dataset, var_name)
        xid = dataset.attrs["_xpublish_id"]
        ds, clims = await self.prepare_ds(dataset, var_name, coords)
        print(ds)
        print(clims)
        plot, html_bytes = None, None
        plotopts = dict(c=var_name, clim=clims, cmap=cmap, label=title, clabel=clabel)
        if not coords or ((not "lat" in coords) and (not "lon" in coords)):
            plotopts["xlim"] = (-180.0, 180.0)
            plotopts["ylim"] = (-90.0, 90.0)
        elif coords:
            if "lon" in coords:
                plotopts["xlim"] = coords["lon"]
            if "lat" in coords:
                plotopts["ylim"] = coords["lat"]
        if ("ocean" in title or "land" in title) and not "native" in title:
            plotopts["tiles"] = "EsriImagery"

        with Client(zarraddress) as genplot_client:
            html_bytes = io.BytesIO()
            # plot = await client.submit(ds.hvplot,
            # plot = client.compute(ds.hvplot,#(
            if "lon" not in ds.dims:
                plot = ds.hvplot(
                    kind="scatter",
                    hover=False,
                    s=50,
                    **plotopts,
                    **self.hvplotopts,
                )
            #            plot = plot.result()

            #                plot=pn.panel(plot)#.servable()
            else:
                #            plot = await client.submit(ds.hvplot,
                #           plot= client.compute(ds.hvplot,
                if "ocean" in title:
                    oceanopts = copy(self.hvplotopts)
                    #                    oceanopts.update(width=1441)
                    plot = ds.hvplot(kind=kind, project=True, **plotopts, **oceanopts)
                else:
                    print("printed")
                    nooceanopts = copy(self.hvplotopts)
                    plot = ds.hvplot(
                        kind=kind,
                        #                        rasterize=True,
                        project=True,
                        **plotopts,
                        **nooceanopts,
                    )
            #            plot = plot.result()
            plot = pn.panel(plot)
            plot = pn.Column(
                plot,
                pn.pane.HTML(
                    '<h1> Built by <a href="https://gitlab.dkrz.de/data-infrastructure-services/eerie-io/-/blob/main/xpublish/host_dynamic_dsets.py" target="_blank"> DKRZs eerie.cloud </a> <br>'
                    + "The dataset is available via:<br></h1><h5> https://eerie.cloud.dkrz.de/datasets/"
                    + xid
                    + " </h5>"
                ),
            )
            plot.save(html_bytes, fmt="html", embed=True)
            del plot
        return html_bytes

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )

        @router.get(
            "/{var_name}/{kind}/{cmap}/{selection:path}",
            response_class=StreamingResponse,
        )
        async def get_custom_plot(
            var_name: str,
            kind: str,
            cmap: str,
            request: Request,
            selection: str = None,
            dataset: xr.Dataset = Depends(get_dataset),
        ):
            if var_name not in dataset.variables:
                raise HTTPException(
                    status_code=404,
                    detail=f"Variable '{var_name}' not found in dataset",
                )
            if kind not in dataset.hvplot.__all__:
                raise HTTPException(
                    status_code=404, detail=f"Cannot use '{kind}' for plotting"
                )
            if cmap not in hv.plotting.list_cmaps():
                raise HTTPException(
                    status_code=404, detail=f"Variable '{cmap}' not found in colormaps"
                )
            llati = False
            llongi = False
            if not "lat" in dataset.coords:
                if not "latitude" in dataset.coords:
                    raise HTTPException(
                        status_code=404, detail=f"Need 'lat' or 'latitude' coordinate"
                    )
                else:
                    dataset = dataset.rename(latitude="lat")
                    llati = True

            if not "lon" in dataset.coords:
                if not "longitude" in dataset.coords:
                    raise HTTPException(
                        status_code=404, detail=f"Need 'lon' or 'longitude' coordinate"
                    )
                else:
                    dataset = dataset.rename(longitude="lon")
                    llongi = True
            coords = None
            if selection:
                rawcoords = selection.split("/")
                if len(rawcoords) % 2 != 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Variable '{rawcoords}' must have length 4",
                    )
                coords = dict()
                for idx, (coord, coord_range) in enumerate(
                    zip(rawcoords[:-1], rawcoords[1:])
                ):
                    if idx % 2 != 0:
                        continue
                    if coord not in dataset[var_name].coords:
                        raise HTTPException(
                            status_code=404,
                            detail=f"Coordinate '{coord}' not found in dataset",
                        )
                    try:
                        l = coord_range.split("_")[0]
                        h = coord_range.split("_")[1]
                        #                        if coord in ["lat","lon","year","month","day"]:
                        l = int(l)
                        h = int(h)
                        coords[coord] = [l, h]
                    except Exception as e:
                        print(e)
                        raise HTTPException(
                            status_code=404,
                            detail=f"Could not use range {coord_range}. Must be separated by '_' and of type integer.",
                        )

            # script = server_document('http://127.0.0.1:7000/app')
            # plot = self.gen_plot(dataset,var_name,kind=kind,cmap=cmap,coords=coords)
            # pn.serve(
            #        {
            #            '/app': plot.servable()
            #            },
            #        port=7000,
            #        allow_websocket_origin=["127.0.0.1:9000"],
            #        address="127.0.0.1",
            #        show=False
            #        )
            # return templates.TemplateResponse("base.html", {"request": request, "script": script})
            html_bytes = asyncio.get_event_loop().run_until_complete(
                self.gen_plot(dataset, var_name, kind=kind, cmap=cmap, coords=coords)
            )
            #            html_bytes= await self.gen_plot(dataset,var_name,kind=kind,cmap=cmap,coords=coords)

            html_bytes.seek(0)
            return StreamingResponse(html_bytes, media_type="text/html")

        return router
