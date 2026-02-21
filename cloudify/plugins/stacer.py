"""Compatibility shim — real implementation lives in cloudify.stac."""
from cloudify.stac.plugin import Stac  # noqa: F401

__all__ = ["Stac"]
