import xarray as xr
import pandas as pd
from typing import Dict, Tuple

# --- Core processing functions ---

def dataset_stats(ds: xr.Dataset) -> Dict[str, any]:
    """Extract statistics from a single xarray Dataset."""
    stats = {
        "nbytes [TB]": ds.nbytes/1024**4,
        "nvars": len(ds.data_vars),
        "var_names": list(ds.data_vars.keys()),
        "no_of_years": 0,
        "start_year": 0,
        "end_year": 0
    }
    if "time" in ds.coords:
        try:
            years = pd.to_datetime(ds.time.values).year
            stats["no_of_years"] = len(set(years))
            stats["start_year"] = int(years.min())
            stats["end_year"] = int(years.max())     
        except Exception:
            stats["no_of_years"] = 0
    return stats


def build_summary_df(ds_dict: Dict[str, xr.Dataset]) -> pd.DataFrame:
    """Build a DataFrame summarizing key metrics from each dataset."""
    rows = []
    for name, ds in ds_dict.items():
        row = dataset_stats(ds)
        row["dataset_id"] = name
        rows.append(row)
    df = pd.DataFrame(rows)
    return df.set_index("dataset_id")


def print_summary(summary: Dict[str, any]):
    print("\n📊 Dataset Collection Summary")
    print("------------------------------")
    print(f"Number of datasets: {summary['total_datasets']}")
    print(f"Total size: {summary['total_nbytes [TB]']}")
    print(f"Maximum dataset size: {summary['max_nbytes [TB]']}")
    print(f"Unique variable names: {summary['unique_vars_count']}")
    print(f"Minimal start year: {summary['min_year']}")
    print(f"Maximum end year: {summary['max_year']}")
    print(f"Maximum years in a dataset: {summary['max_years']}")

def summarize_overall(df: pd.DataFrame) -> Dict[str, any]:
    """Compute overall statistics from the summary DataFrame."""
    unique_vars = set(var for sublist in df["var_names"] for var in sublist)
    return {
        "total_datasets": len(df),
        "total_nbytes [TB]": df["nbytes [TB]"].sum(),
        "max_nbytes [TB]": df["nbytes [TB]"].max(),
        "unique_vars_count": len(unique_vars),
        "min_year": df["start_year"].min(),
        "max_year": df["end_year"].max(),
        "max_years": df["no_of_years"].max()
    }
