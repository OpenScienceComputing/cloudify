import panel as pn
import pandas as pd
import glob
from xpublish import Plugin, hookimpl, Dependencies
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Sequence
import io
from bokeh.models.widgets.tables import HTMLTemplateFormatter
import re
import time
from collections import defaultdict

LOG_PATH = "/var/log/nginx/access.log"
#LOG_PATH = "/tmp/nginx_access.log"
WINDOW_SECONDS = 3600  # last hour

log_re = re.compile(
    r'(?P<ip>\S+) - - \[(?P<date>[^\]]+)] '
    r'"GET (?P<path>\S+) [^"]+" (?P<status>\d{3}) (?P<bytes>\d+)'
)

def parse_dataset(path: str) -> str:
    parts = path.strip("/").split("/")
    if len(parts) >= 2:
        return parts[1]
    return "unknown"


def iter_reverse_lines(filename, chunk_size=4096):
    """Yield lines from a file in reverse order (efficient)."""
    with open(filename, "rb") as f:
        f.seek(0, 2)
        buffer = b""
        pos = f.tell()

        while pos > 0:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size)
            buffer = data + buffer
            *lines, buffer = buffer.split(b"\n")
            for line in reversed(lines):
                yield line.decode("utf-8", "ignore")

        if buffer:
            yield buffer.decode("utf-8", "ignore")

def get_stats():
    now = time.time()
    cutoff = now - WINDOW_SECONDS

    total_bytes = 0
    ips = set()
    dataset_bytes = defaultdict(int)
    dataset_reqs = defaultdict(int)

    # Iterate *backwards* through the log so we can stop early
    for line in iter_reverse_lines(LOG_PATH):

        m = log_re.search(line)
        if not m:
            continue

        # Filter by status == 200
        status = int(m.group("status"))
        if status != 200:
            continue

        # Parse Nginx timestamp: "09/Dec/2025:13:52:18 +0100"
        raw_date = m.group("date")
        # fast parsing of timestamp
        ts = time.mktime(time.strptime(raw_date[:20], "%d/%b/%Y:%H:%M:%S"))

        # Stop as soon as we leave the 1-hour window
        if ts < cutoff:
            break

        path = m.group("path")
        bytes_sent = int(m.group("bytes"))
        ip = m.group("ip")
        dataset = parse_dataset(path)

        total_bytes += bytes_sent
        ips.add(ip)
        dataset_bytes[dataset] += bytes_sent
        dataset_reqs[dataset] += 1

    top_bytes = sorted(dataset_bytes.items(), key=lambda x: x[1], reverse=True)[:10]
    top_reqs = sorted(dataset_reqs.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "window_seconds": WINDOW_SECONDS,
        "unique_ips": len(ips),
        "total_GB": total_bytes / (1024**3),
        "top10_datasets_by_bytes": [
            {"dataset": d, "bytes": b} for d, b in top_bytes
        ],
        "top10_datasets_by_requests": [
            {"dataset": d, "requests": r} for d, r in top_reqs
        ],
    }

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
            status_code=404, detail=f"{fn} not found"
        )

def create_tabulator_html(df:pd.DataFrame, l_explode:bool=False):
    html_bytes=io.BytesIO()
    stacurl="https://discover.dkrz.de/external/eerie.cloud.dkrz.de/datasets/"
    
    link_formatter = HTMLTemplateFormatter(
        template=f'<a target="_blank" href="{stacurl}/<%= value %>/stac"><%= value %></a>'
    )
    
    if l_explode:
        df = df.explode("var_names", ignore_index=True)   
        for a in ["nvars", "nbytes [TB]","start_year","end_year", "no_of_years"]:
            if a in df.columns:
                del df[a]
        df["aggregation"]=df["dataset_id"].str.split('.').str[-1]
    
        
    tabu = pn.widgets.Tabulator(
        df,
        show_index=False,
        widths={"var_names": 200},
        formatters={"dataset_id": link_formatter},
        #hidden_columns=["stac"],
        pagination ="local",
        header_filters=True,
        selectable=1,
        page_size=20,
        editors={
            a:None
            for a in df.columns
        }        
    )
    filename, button = tabu.download_menu(
        text_kwargs={'name': 'Enter filename', 'value': 'default.csv'},
        button_kwargs={'name': 'Download table'}
    )
    
    out=pn.Row(
        pn.Column(filename, button),
        tabu
    )    

    out.save(html_bytes, fmt="html", embed=True)
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

        @router.get("-summary.html", summary="Statistics over all datasets")
        def get_summary(
            dataset_ids=Depends(deps.dataset_ids)
        ):
            dfs = []
            try:
                for dfsource in sorted(glob.glob("/tmp/*datasets.csv")):
                    dfs.append(read_csv(dfsource))
                df=pd.concat(dfs)
                sumdf=summarize_overall(df)
                html_bytes = create_tabulator_html(sumdf)
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            return StreamingResponse(html_bytes, media_type="text/html")

        @router.get("-summary.csv", summary="Statistics over all datasets")
        def get_summary(
            dataset_ids=Depends(deps.dataset_ids)
        ):
            dfs = []
            try:
                for dfsource in sorted(glob.glob("/tmp/*datasets.csv")):
                    dfs.append(read_csv(dfsource))
                df=pd.concat(dfs)
                sumdf=summarize_overall(df)

                stream = io.StringIO()
                df.to_csv(stream, index = False)
                response = StreamingResponse(iter([stream.getvalue()]),
                                     media_type="text/csv"
                                    )
                response.headers["Content-Disposition"] = "attachment; filename=summary.csv"
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            
            return response            

        @router.get("-summary-{project}.html", summary="Statistics over all datasets for a project")
        def get_summary_project(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            try:
                df = read_csv(f"/tmp/{project}_datasets.csv")

                sumdf=summarize_overall(df)
                html_bytes = create_tabulator_html(sumdf)
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            
            return StreamingResponse(html_bytes, media_type="text/html")

        @router.get("-summary-{project}.csv", summary="Statistics over all datasets for a project as csv")
        def get_summary_project_csv(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            try:
                df = read_csv(f"/tmp/{project}_datasets.csv")

                sumdf=summarize_overall(df)

                stream = io.StringIO()    
                sumdf.to_csv(stream, index = False)    
                response = StreamingResponse(
                        iter([stream.getvalue()]),
                        media_type="text/csv"
                        )
                response.headers["Content-Disposition"] = f"attachment; filename=summary-{project}.csv"  
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            
            return response

        @router.get("-{project}.html", summary="Statistics for a project")
        def get_project(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            try:
                df = read_csv(f"/tmp/{project}_datasets.csv")
                html_bytes = create_tabulator_html(df, l_explode=True)
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            
            return StreamingResponse(html_bytes, media_type="text/html")
        
        @router.get("-{project}.csv", summary="Statistics for a project")
        def get_project_csv(
            project:str, dataset_ids=Depends(deps.dataset_ids)
        ):
            try:
                df = read_csv(f"/tmp/{project}_datasets.csv")
                stream = io.StringIO()
                df.to_csv(stream, index = False)
                response = StreamingResponse(
                        iter([stream.getvalue()]),        
                        media_type="text/csv"
                        )
                response.headers["Content-Disposition"] = f"attachment; filename={project}.csv"
            except:
                return HTTPException(status_code=404, detail="Could not create table")
            
            return response           

        @router.get("_nginx.json", summary="NGINX metrics")
        def get_nginx():            
            #try:
            if True:
                response = JSONResponse(get_stats())
            #except:
            #    return HTTPException(status_code=404, detail="Could not create table")

            return response

        return router

