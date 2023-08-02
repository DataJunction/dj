"""
Tests for the djql API.
"""
# pylint: disable=too-many-lines
from typing import Dict, List, Optional
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from datajunction_server.models.node import Node

   
def test_get_djql_data_only_nodes_query(
    client_with_examples: TestClient,
) -> None:
    """
    Test djql with just some non-metric nodes
    """
    
    query = """
    SELECT default.hard_hat.country, 
      default.hard_hat.city  
    """
    
    response = client_with_examples.get(
        "/djql/data/",
        params={
            "query": query
        },
    )
    data = response.json()
    assert response.status_code == 422
    assert data["message"] == "Cannot set dimensions for node type dimension!"    
