from typing import Dict
import math

FREQUENCY_MAPPING = {
    "10m": "10m",
    "15m": "15m",
    "3h": "3hr",
    "3hr": "3hr",
    "6h": "6hr",
    "6hr": "6hr",
    "1h": "1hr",
    "1hr": "1hr",
    "4h": "4hr",
    "4hr": "4hr",
    "24h": "daily",
    "1d": "daily",
    "day": "daily",
    "daily" : "daily",
    "mon": "monthly",
    "p1m": "monthly",
    "monthly": "monthly",
    "1m": "monthly",
    "1y": "yearly",
    "year": "yearly",
    "yearly": "yearly",
    "grid": "fx"
}

GRID_LABEL_MAPPING = {
    "native":"gn",
    "gr025":"gr",
    "gn":"gn",
    "gr":"gr"
}

REALM_MAPPING = {
    "atmos":"atmos",
    "atm":"atmos",
    "oce":"ocean",
    "ocean":"ocean",
    "land":"land"
}

def set_realm(item_title:str, item_ds):
    c=next((b for b in item_title.split(' ') if b in list(REALM_MAPPING.keys())), None)
    if c:
        item_ds.attrs["realm"]=REALM_MAPPING[c]
        return item_ds 

    preset=item_ds.attrs.get("realm")

    if preset and preset in REALM_MAPPING:
        item_ds.attrs["realm"]=REALM_MAPPING[preset]
    elif preset not in list(REALM_MAPPING.values()):
        item_ds.attrs["realm"] = "all"  # Default value if no match is found
    return item_ds


def set_grid_label(item_title:str, item_ds):
    c=next((b for b in item_title.split(' ') if b in list(GRID_LABEL_MAPPING.keys())), None)
    if c:
        item_ds.attrs["grid_label"]=GRID_LABEL_MAPPING[c]
        return item_ds 

    preset=item_ds.attrs.get("grid_label")

    if preset and preset in GRID_LABEL_MAPPING:
        item_ds.attrs["grid_label"]=GRID_LABEL_MAPPING[preset]
    elif preset not in list(GRID_LABEL_MAPPING.values()):
        item_ds.attrs["grid_label"] = "unknown"  # Default value if no match is found
    return item_ds


def set_frequency(item_title:str, item_ds):
    item_splits=item_title.split(' ')
    c=next((FREQUENCY_MAPPING.get(b) for b in item_splits if b in FREQUENCY_MAPPING),None)
    if c:
        item_ds.attrs["frequency"]=c
        return item_ds 

    inp=next(
        (
            a for a in item_splits
            if a.startswith("PT") or a.startswith("P1")
        ),
        None
    )
    if inp:
        mapping_key=next((a for a in FREQUENCY_MAPPING if a in inp.lower()),None)
        if mapping_key:
            item_ds.attrs["frequency"] = FREQUENCY_MAPPING[mapping_key]
            
    preset=item_ds.attrs.get("frequency")

    if preset :
        if preset in FREQUENCY_MAPPING:
            item_ds.attrs["frequency"]=FREQUENCY_MAPPING[preset]
        elif preset not in list(FREQUENCY_MAPPING.values()):
            item_ds.attrs["frequency"] = "unknown"  # Default value if no match is found
    return item_ds

def map_expname_to_experiment(local_conf: Dict[str, str], exp_name_lower: str) -> Dict[str, str] :
    if "d3hp" in exp_name_lower:
        if "aug" in exp_name_lower:
            local_conf["experiment_id"]="d3hp003aug" if not local_conf.get("experiment_id") else local_conf["experiment_id"]+"-aug"
        if "feb" in exp_name_lower:
            local_conf["experiment_id"]="d3hp003feb" if not local_conf.get("experiment_id") else local_conf["experiment_id"]+"-feb"
        local_conf["experiment_id"]+="-amip"
    if "rcbmf" in exp_name_lower:
        local_conf["experiment_id"]="rcbmf" if not local_conf.get("experiment_id") else local_conf["experiment_id"]+"-rcbmf"
        if "ng5" not in exp_name_lower and "fesom" not in exp_name_lower:
            local_conf["experiment_id"]+="-amip"
    if "deepoff" in exp_name_lower:
        local_conf["experiment_id"]="deepoff" if not local_conf.get("experiment_id") else local_conf["experiment_id"]+"-deepoff"
        if "ng5" not in exp_name_lower and "fesom" not in exp_name_lower:
            local_conf["experiment_id"]+="-amip"        
    if "_cf" in exp_name_lower:
        local_conf["experiment_id"]+="-refined"
    if "ngc4" in exp_name_lower:
        local_conf["experiment_id"]="ngc4008"
        
    return local_conf

def map_source_to_institution(local_conf: dict):
    if not local_conf.get("institution_id"):
        lower_model=local_conf["source_id"].lower()
        if "icon" in lower_model:
            local_conf["institution_id"]="MPI-M"
        elif "ifs" in lower_model:
            if "fesom" in lower_model:
                local_conf["institution_id"]="ECMWF-AWI"
            elif "nemo" in lower_model:
                local_conf["institution_id"]="BSC"
            else:
                local_conf["institution_id"]="ECMWF"
            if "reanalysis" in local_conf.get("experiment_id","None"):
                local_conf["institution_id"]+="-DKRZ"
        elif "merra" in lower_model:
            local_conf["institution_id"]="NASA-GMAO"
        elif "jra-3q" in lower_model:
            local_conf["institution_id"]="JMA"
        elif "cas-esm" in lower_model:
            local_conf["institution_id"]="CAS"
        elif "scream" in lower_model:
            local_conf["institution_id"]="E3SM-Project"            
        elif "ceres" in lower_model:
            local_conf["institution_id"]="NASA-GMAO"
        elif "arp-gem" in lower_model:
            local_conf["institution_id"]="CNRM"
        else:
            local_conf["institution_id"]="Unknown"

    return local_conf

def map_expname_to_source(local_conf: Dict[str, str], exp_name_lower: str) -> Dict[str, str] :
    """
    Map experiment name to appropriate source ID and institution ID based on naming conventions.
    
    Args:
        local_conf: Configuration dictionary containing STAC metadata
        exp_name_lower: Lowercase experiment name to map
        
    Returns:
        Updated configuration dictionary with source_id and institution_id set
    """
    if not local_conf.get("source_id"): 
        fi=local_conf.get("from_intake",{})
        if "icon" in exp_name_lower:
            local_conf["source_id"]="ICON"
            if "d3hp" in exp_name_lower:
                local_conf["source_id"]+="-R2B10"
        elif "scream" in exp_name_lower:
            local_conf["source_id"]="SCREAM"
        elif "jra3q" in exp_name_lower:
            local_conf["source_id"]="JRA-3Q"
            local_conf["experiment_id"]="reanalysis"
            local_conf["frequency"]="monthly"
        elif "casesm" in exp_name_lower:
            local_conf["source_id"]="CAS-ESM"
        elif "ifs" in exp_name_lower:
            local_conf["source_id"]="IFS"
            if "tco3999" in exp_name_lower:
                local_conf["source_id"]+="-TCO3999"
            elif "tco2559" in exp_name_lower:
                local_conf["source_id"]+="-TCO2559"
            elif "tco1279" in exp_name_lower:
                local_conf["source_id"]+="-TCO1279"
            elif "tco399" in exp_name_lower:
                local_conf["source_id"]+="-TCO399"
            if "fesom" in exp_name_lower or "ng5" in exp_name_lower: #ng5 only in fesom?
                local_conf["source_id"]+="-FESOM2" # only fesom2 available?!
                if "ng5" in exp_name_lower:
                    local_conf["source_id"]+="-NG5"
            if local_conf["source_id"]=="IFS" :
                if fi.get("source_id"):
                    local_conf["source_id"]=fi.get("source_id")
                else:
                    print(local_conf["from_intake"])
        elif "merra" in exp_name_lower:
            local_conf["source_id"]="MERRA2"
            local_conf["experiment_id"]="reanalysis"  
            local_conf["frequency"]="monthly"                        
        elif "ceres" in exp_name_lower:
            local_conf["source_id"]="CERES-EBAF"
        elif "arp-gem-1" in exp_name_lower:
            local_conf["source_id"]="ARP-GEM2-TCO7679"
        elif "arp-gem-2" in exp_name_lower:
            local_conf["source_id"]="ARP-GEM2-TCO3839"
        elif ".era" in exp_name_lower:
            local_conf["source_id"]="IFS"
            local_conf["experiment_id"]="reanalysis"
            local_conf["frequency"]="monthly"                        
        else:
            local_conf["source_id"]=exp_name_lower.strip('.').strip('_').strip(':').strip('-')
    return local_conf

def get_and_set_zoom(item_json:dict)->dict:
    zoomstr=None
    if item_json["properties"].get("zoom"):
        item_json["properties"]["grid_label"]="gr"        
        return item_json
    source_id=item_json["properties"].get("source_id")
    iid=item_json.get("id")
    if any(source_id==b for b in ["ICON","ICON-LAM"]):
        zoomstr=iid.split('_')[-1].strip('z').strip('.json')
        if zoomstr.isnumeric():
            item_json["properties"]["zoom"]=int(zoomstr)
    elif "healpix" in iid:
        try:
            zoomstr=iid.split('healpix')[-1].split(' ')[0].strip('.').strip('_snow').strip('_ocea')
            if zoomstr.isnumeric():
                item_json["properties"]["zoom"]=int(math.log2(int(zoomstr)))
        except:
            print(f"Could not find out healpix level for id {item_json['id']}")
    else:
        print(f"Could not find out healpix level for id {item_json['id']}")
    if item_json["properties"].get("zoom"):
        item_json["properties"]["grid_label"]="gr"
    return item_json
