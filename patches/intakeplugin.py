import logging
from copy import deepcopy as copy
from typing import Sequence

import yaml
from fastapi import APIRouter, Depends, Request, Response
from starlette.routing import NoMatchFound
from xpublish.plugins import Dependencies, Plugin, hookimpl
from xpublish.utils.api import DATASET_ID_ATTR_KEY

logger = logging.getLogger('intake_catalog')


def get_dataset_id(ds,url):
    xpublish_id = ds.attrs.get(DATASET_ID_ATTR_KEY)
    cf_dataset_id = ".".join(
        [
            x for x in [
                ds.attrs.get('naming_authority'),
                ds.attrs.get('id')
            ] if x
        ]
    )

    dataset_id_by_url = url.split('/')[-2]
    dataset_id_options = [
        xpublish_id,
        dataset_id_by_url,
        cf_dataset_id,
        'dataset'
    ]

    return next(x for x in dataset_id_options if x)


def get_zarr_source(xpublish_id, dataset, request):
    url = ''
    try:
        from xpublish.plugins.included.zarr import ZarrPlugin  # noqa
        url = request.url_for("get_zarr_metadata")
    except NoMatchFound:
        # On multi-dataset servers add the dataset_id to the route
        url = request.url_for("get_zarr_metadata", dataset_id=xpublish_id)

    # Convert url object from <class 'starlette.datastructures.URL'> to a string
    url = str(url)

    # Remove .zmetadata from the URL to get the root zarr URL
    url = url.replace("/.zmetadata", "")

    dataset_id_by_url = url.split('/')[-2]
    l_consolidated=True    
    #if "native" in url:
    #    l_consolidated=False
#    if "remap" in url:
#        url = "reference::"+'/'.join(url.split('/')[0:-3])+"/static/kerchunks2/"+dataset_id_by_url+".json"
#        l_consolidated=False
    if not url:
        return {}

    return {
        'driver': 'zarr',
        'description': dataset.attrs.get('summary', ''),
        'args': {
            'consolidated': l_consolidated,
            'urlpath': url.replace("http://","https://")
        }
    }


class IntakePlugin(Plugin):
    """Adds an Intake catalog endpoint"""

    name : str = 'intake_catalog'
    dataset_metadata : dict = dict()

    app_router_prefix: str = '/intake'
    app_router_tags: Sequence[str] = ['intake']

    dataset_router_prefix: str = ''
    dataset_router_tags: Sequence[str] = ['intake']

    @hookimpl
    def app_router(self, deps: Dependencies):
        """Register an application level router for app level intake catalog"""
        router = APIRouter(prefix=self.app_router_prefix, tags=self.app_router_tags)

        def get_request(request: Request) -> str:
            return request

        @router.get(".yaml", summary="Root intake catalog")
        def get_root_catalog(
            request=Depends(get_request),
            dataset_ids = Depends(deps.dataset_ids)
        ):

            data = {
                'metadata': {
                    'source': 'Served via `xpublish-intake`',
                    'access_url': str(request.url).replace("http://","https://"),
                }
            }

            if dataset_ids:
#                data['sources'] = {
#                    d: {
#                        'description': self.dataset_metadata.get(d, {}).get('description', ''),
#                        'driver': 'intake.catalog.local.YAMLFileCatalog',
#                        'metadata': self.dataset_metadata.get(d, {}),
#                        'args': {
#                            'path': str(request.url_for('get_dataset_catalog', dataset_id=d)).replace("http://","https://")
#                        }
#                    }
#                    for d in dataset_ids
                data['sources'] = {
                    d: {
                        'description': self.dataset_metadata.get(d, {}).get('description', ''),
                        'driver': 'zarr',
                        'metadata': self.dataset_metadata.get(d, {}),
                        'args': {
                            'urlpath': str(request.url_for('get_dataset_catalog', dataset_id=d)).replace("http://","https://").replace("catalog.yaml","{{ method }}"),
                            'consolidated':True
                        },
                        'parameters':dict(
                            method=dict(
                                allowed=["kerchunk","zarr"],
                                default="kerchunk",
                                type="str",
                                description="server-side loading method"
                                )
                            )
                    }
                    for d in dataset_ids
                }
            else:
                data['sources'] = {
                    'dataset': {
                        'description': self.dataset_metadata.get('default', {}).get('description', ''),
                        'driver': 'intake.catalog.local.YAMLFileCatalog',
                        'metadata': self.dataset_metadata.get('default', {}),
                        'args': {
                            'path': str(request.url_for('get_dataset_catalog')).replace("http://","https://")
                        }
                    }
                }

            return Response(yaml.dump(data), media_type="text/yaml")

        return router

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags))

        def get_request(request: Request) -> str:
            return request

        @router.get('/catalog.yaml', summary="Dataset intake catalog")
        def get_dataset_catalog(
            request=Depends(get_request),
            dataset=Depends(deps.dataset),
        ):
            xpublish_id = get_dataset_id(dataset,str(request.url))
            sources = {
                'zarr': get_zarr_source(xpublish_id, dataset, request),
            }
            if "source" in dataset.encoding:
                sources.update(
                        {
                            'kerchunk':copy(sources['zarr'])
                        }
                        )
                sources['kerchunk']['args']['urlpath']=sources['kerchunk']['args']['urlpath'].replace('zarr','kerchunk')

            data = {
                'name': xpublish_id,
                'metadata': {
                    'source': 'Served via `xpublish-intake`',
                    'access_url': str(request.url).replace("http://","https://"),
                },
                'sources': {
                    f'{xpublish_id}-{k}': v for k, v in sources.items() if v
                }
            }

            return Response(yaml.dump(data), media_type="text/yaml")

        return router
