"""Models for materialization"""
from typing import Dict, List, Optional

from pydantic import AnyHttpUrl, BaseModel


class GenericMaterializationInput(BaseModel):
    """
    The input when calling the query service's materialization
    API endpoint for a generic node.
    """

    name: str
    node_name: str
    node_type: str
    schedule: str
    query: str
    upstream_tables: List[str]
    spark_conf: Optional[Dict] = None
    partitions: Optional[List[Dict]] = None


class DruidMaterializationInput(GenericMaterializationInput):
    """
    The input when calling the query service's materialization
    API endpoint for a cube node.
    """

    druid_spec: Dict


class MaterializationOutput(BaseModel):
    """
    The output when calling the query service's materialization
    API endpoint for a cube node.
    """

    urls: List[AnyHttpUrl]
