"""cloudify.stac — Generic STAC builder for cloud-hosted model datasets."""

from cloudify.stac.builder import (
    ICECHUNK_MEDIA_TYPE,
    ICECHUNK_STAC_EXTENSIONS,
    build_datacube_extension,
    build_stac_collection,
    build_stac_item,
    build_stac_item_from_icechunk,
    extract_spatial_extent,
    extract_spatial_extent_rio,
    extract_temporal_extent,
    load_config,
    make_json_serializable,
    merge_configs,
)

__all__ = [
    "ICECHUNK_MEDIA_TYPE",
    "ICECHUNK_STAC_EXTENSIONS",
    "build_datacube_extension",
    "build_stac_collection",
    "build_stac_item",
    "build_stac_item_from_icechunk",
    "extract_spatial_extent",
    "extract_spatial_extent_rio",
    "extract_temporal_extent",
    "load_config",
    "make_json_serializable",
    "merge_configs",
    "Stac",
]


def __getattr__(name: str):
    if name == "Stac":
        from cloudify.stac.plugin import Stac  # noqa: PLC0415
        return Stac
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
