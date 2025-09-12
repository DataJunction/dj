from typing import List
from pydantic import BaseModel


class ImpactedNode(BaseModel):
    name: str
    caused_by: List[str]


class ImpactedNodes(BaseModel):
    downstreams: List[ImpactedNode]
    links: List[ImpactedNode]


class HardDeleteResponse(BaseModel):
    deleted_nodes: list[str]
    deleted_namespaces: list[str]
    impacted: ImpactedNodes
