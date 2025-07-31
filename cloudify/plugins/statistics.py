import panel as pn
import pandas as pd
import glob
from xpublish import Plugin, hookimpl, Dependencies
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from typing import Sequence
from io import BytesIO

def summarize_overall(df: pd.DataFrame) -> pd.DataFrame:
    """Compute overall statistics from the summary DataFrame."""
    unique_vars = set(var for sublist in df["var_names"] for var in sublist)
    return pd.DataFrame({
        "total_datasets": len(df),
        "total_nbytes [TB]": df["nbytes [TB]"].sum(),
        "max_nbytes [TB]": df["nbytes [TB]"].max(),
        "unique_vars_count": len(unique_vars),
        "min_year": df["start_year"].min(),
        "max_year": df["end_year"].max(),
        "max_years": df["no_of_years"].max()
    },index=[0])

def read_csv(fn:str) -> pd.DataFrame:
    try:
        df = pd.read_csv(fn)
        df["var_names"] = df["var_names"].apply(lambda s: eval(s))
        return df
    except:
        raise HTTPException(
            status_code=404, detail=f"{project} not found"
        )

def create_tabulator_html(df:pd.DataFrame):
    html_bytes=BytesIO()
    tabu = pn.widgets.Tabulator(
        df,
        #show_index=False,
        widths={"var_names": 200}
    )
    tabu.save(html_bytes, fmt="html", embed=True)
    html_bytes.seek(0)    
    return html_bytes

class Stats(Plugin):
    name: str = "statistics"
    mapper_dict: dict = {}

    app_router_prefix: str = "/stats"
    app_router_tags: Sequence[str] = ["stats"]

    dataset_router_prefix: str = "/stats"
    dataset_router_tags: Sequence[str] = ["stats"]

    @hookimpl
    def app_router(self, deps: Dependencies):
        """Register an application level router for app level stac catalog"""
        router = APIRouter(prefix=self.app_router_prefix, tags=self.app_router_tags)

        @router.get("-summary", summary="Statistics over all datasets")
        def get_eerie_collection(
            dataset_ids=Depends(deps.dataset_ids)
        ):
            dfs = []
            for dfsource in sorted(glob.glob("*datasets.csv")):
                dfs.append(read_csv(dfsource))
            df=pd.concat(dfs)
            sumdf=summarize_overall(df)
            html_bytes = create_tabulator_html(sumdf)
            return StreamingResponse(html_bytes, media_type="text/html")

        @router.get("-summary_{project}", summary="Statistics over all datasets for a project")
        def get_eerie_collection(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            df = read_csv(f"{project}_datasets.csv")

            sumdf=summarize_overall(df)
            html_bytes = create_tabulator_html(sumdf)
            return StreamingResponse(html_bytes, media_type="text/html")

        @router.get("-{project}", summary="Statistics for a project")
        def get_eerie_collection(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            df = read_csv(f"{project}_datasets.csv")
            html_bytes = create_tabulator_html(df)
            return StreamingResponse(html_bytes, media_type="text/html")
        
        return router

