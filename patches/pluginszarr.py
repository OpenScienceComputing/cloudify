import logging
from typing import Sequence
from datetime import datetime
todaystring=datetime.today().strftime("%a, %d %b %Y %H:%M:%S GMT")

import cachey  # type: ignore
import xarray as xr
from fastapi import APIRouter, Depends, HTTPException, Path, Request
from starlette.responses import Response  # type: ignore
from numcodecs.abc import Codec

from xpublish.utils.api import JSONResponse

from ...utils.api import DATASET_ID_ATTR_KEY
from ...utils.cache import CostTimer
from ...utils.zarr import (
    ZARR_METADATA_KEY,
    array_meta_key,
    attrs_key,
    encode_chunk,
    get_data_chunk,
    get_zmetadata,
    get_zvariables,
    group_meta_key,
    jsonify_zmetadata,
)

# type: ignore
from .. import Dependencies, Plugin, hookimpl

logger = logging.getLogger('zarr_api')

def validate_dask_arrays(dataset):
    """Raise an error if any data variable is not a Dask array"""
    non_dask_vars = [
        name for name, var in dataset.data_vars.items()
        if var.chunks is None
    ]
    if non_dask_vars:
        raise HTTPException(
            status_code=400,
            detail=f"The following variables are not Dask arrays: {', '.join(non_dask_vars)}"
        )

class ZarrPlugin(Plugin):
    """Adds Zarr-like accessing endpoints for datasets."""

    name: str = 'zarr'

    dataset_router_prefix: str = '/zarr'
    dataset_router_tags: Sequence[str] = ['zarr']

    @hookimpl
    def dataset_router(self, deps: Dependencies) -> APIRouter:  # noqa: D102
        router = APIRouter(
            prefix=self.dataset_router_prefix,
            tags=list(self.dataset_router_tags),
        )

        @router.api_route(f'/{ZARR_METADATA_KEY}',methods=['GET', 'HEAD'])        
        def get_zarr_metadata(
            dataset=Depends(deps.dataset),
            cache=Depends(deps.cache),
        ) -> dict:
            """Consolidated Zarr metadata."""
            validate_dask_arrays(dataset)            
            zvariables = get_zvariables(dataset, cache)
            zmetadata = get_zmetadata(dataset, cache, zvariables)

            zjson = jsonify_zmetadata(dataset, zmetadata)

            return JSONResponse(zjson)

        @router.api_route(f'/{group_meta_key}',methods=['GET', 'HEAD'])        
        def get_zarr_group(
            dataset=Depends(deps.dataset),
            cache=Depends(deps.cache),
        ) -> dict:
            """Zarr group data."""
            validate_dask_arrays(dataset)            
            zvariables = get_zvariables(dataset, cache)
            zmetadata = get_zmetadata(dataset, cache, zvariables)

            return JSONResponse(zmetadata['metadata'][group_meta_key])

        @router.api_route(f'/{attrs_key}',methods=['GET', 'HEAD'])        
        def get_zarr_attrs(
            dataset=Depends(deps.dataset),
            cache=Depends(deps.cache),
        ) -> dict:
            """Zarr attributes."""
            validate_dask_arrays(dataset)
            zvariables = get_zvariables(dataset, cache)
            zmetadata = get_zmetadata(dataset, cache, zvariables)

            return JSONResponse(zmetadata['metadata'][attrs_key])

        @router.api_route('/{var}/{chunk}',methods=['GET', 'HEAD'])        
        def get_variable_chunk(
            request: Request,                
            var: str = Path(description='Variable in dataset'),
            chunk: str = Path(description='Zarr chunk'),
            dataset: xr.Dataset = Depends(deps.dataset),
            cache: cachey.Cache = Depends(deps.cache),
        ):
            """Get a zarr array chunk.

            This will return cached responses when available.

            """
            if var not in dataset.variables:
                raise HTTPException(status_code=404, detail='Not in dataset')            
            validate_dask_arrays(dataset)
            zvariables = get_zvariables(dataset, cache)
            app=request.app
            zmetadata = get_zmetadata(dataset, cache, zvariables)
            # First check that this request wasn't for variable metadata
            if array_meta_key in chunk:
                newzmeta=copy.deepcopy(zmetadata['metadata'][f'{var}/{array_meta_key}'])
                newzmeta.pop("filters")
                compressor=newzmeta.get("compressor")
                if compressor and isinstance(compressor,Codec):
                    correct_comprdict=compressor.get_config()
                    del newzmeta['compressor']
                    newzmeta['compressor']=correct_comprdict
                return newzmeta            
            elif attrs_key in chunk:
                return JSONResponse(zmetadata['metadata'][f'{var}/{attrs_key}'])
            elif group_meta_key in chunk:
                raise HTTPException(status_code=404, detail='No subgroups')
            else:
                logger.debug('var is %s', var)
                logger.debug('chunk is %s', chunk)

                #cache_key = dataset.attrs.get(DATASET_ID_ATTR_KEY, '') + '/' + f'{var}/{chunk}'
                #response = cache.get(cache_key)

                #if response is None:
                    #with CostTimer() as ct:
                arr_meta = zmetadata['metadata'][f'{var}/{array_meta_key}']
                da = zvariables[var].data

                data_chunk = get_data_chunk(
                        app.state.dask_client,
                        da,
                        chunk,
                        out_shape=arr_meta['chunks'],
                        filters=arr_meta['filters'],
                        compressor=arr_meta['compressor'],                        
                        )
                if not isinstance(data_chunk, bytes):
                    data_chunk = data_chunk.tobytes()

                #Done by dask
                #echunk = encode_chunk(
                #        data_chunk,
                #        filters=arr_meta['filters'],
                #        compressor=arr_meta['compressor'],
                #        )

                response = Response(
                        data_chunk,
                        media_type='application/octet-stream',
                        )

                response.headers["Cache-control"] = "max-age=3600"
                response.headers["X-EERIE-Request-Id"] = "True"
                response.headers["Last-Modified"] = datetime.today().strftime(
                        "%a, %d %b %Y 00:00:00 GMT"
                        )
                response.headers["Access-Control-Allow-Origin"] = "*"
                response.headers["Access-Control-Allow-Methods"] = "POST,GET,HEAD"
                response.headers["Access-Control-Allow-Headers"] = "*"
                    #cache.put(cache_key, response, ct.time, len(echunk))

                return response

        return router
