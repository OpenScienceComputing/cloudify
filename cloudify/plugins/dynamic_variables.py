from typing import Sequence
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
import xarray as xr

from xpublish.dependencies import (
    get_dataset,
)  # Assuming 'dependencies' is the module where get_dataset is defined
from xpublish import Plugin, hookimpl, Dependencies


class DynamicAdd(Plugin):
    name: str = "dynamic-add"

    dataset_router_prefix: str = "/groupby"
    dataset_router_tags: Sequence[str] = ["groupby"]

    @hookimpl
    def dataset_router(self, deps: Dependencies):
        router = APIRouter(
            prefix=self.dataset_router_prefix, tags=list(self.dataset_router_tags)
        )

        @router.get("/{variable_name}/{groupby_coord}/{groupby_action}")
        def evaluate_groupby(
            variable_name: str,
            groupby_coord: str,
            groupby_action: str,
            request: Request,
            dataset: xr.Dataset = Depends(get_dataset),
        ):
            if variable_name not in dataset.variables:
                raise HTTPException(
                    status_code=404,
                    detail=f"Variable '{variable_name}' not found in dataset",
                )

            coord_name = groupby_coord.split(".")[0]
            if coord_name not in dataset.dims and coord_name not in dataset.coords:
                raise HTTPException(
                    status_code=404,
                    detail=f"Groupby coordinate '{groupby_coord}' not found in dataset",
                )

            if groupby_action not in [
                "mean",
                "min",
                "max",
                "count",
                "first",
                "last",
                "sum",
            ]:
                raise HTTPException(
                    status_code=404,
                    detail=f"Groupby action '{groupby_action}' not recognized",
                )

            groupby_expression = f"dataset['{variable_name}'].groupby('{groupby_coord}').{groupby_action}()"
            result_dataset = eval(groupby_expression)
            new_variable_name = f"{variable_name}_{groupby_coord}_{groupby_action}"

            if new_variable_name not in dataset:
                exec(f"dataset['pp_{new_variable_name}'] = {groupby_expression}")

            with xr.set_options(display_style="html"):
                return HTMLResponse(result_dataset._repr_html_())

        return router
