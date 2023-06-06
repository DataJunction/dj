"""Models for materialization"""
from typing import Dict, List, Optional

from pydantic import BaseModel


class GenericMaterializationInput(BaseModel):
    """
    The expected input when calling the query service's materialization
    API endpoint for a generic node.
    """

    node_name: str
    node_type: str
    schedule: str
    query: str
    upstream_tables: List[str]
    spark_conf: Optional[Dict] = None
    partitions: Optional[List[Dict]] = None


class DruidMaterializationInput(GenericMaterializationInput):
    """
    The expected input when calling the query service's materialization
    API endpoint for a cube node.
    """

    druid_spec: Dict
