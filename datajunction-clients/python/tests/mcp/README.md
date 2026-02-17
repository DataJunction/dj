# MCP Tools Test Coverage

## Summary

This test suite provides comprehensive coverage for the DataJunction MCP (Model Context Protocol) server tools.

**Current Coverage: 98%**
- Total: 35 test cases
- All tests passing ✓

## Test Categories

### 1. DJGraphQLClient Tests (11 tests)
Tests for the GraphQL client that handles authentication and API communication:

- ✓ Token-based authentication
- ✓ Username/password login flow
- ✓ Login failures and error handling
- ✓ GraphQL error responses
- ✓ HTTP errors (4xx, 5xx)
- ✓ Connection errors
- ✓ Edge cases (no credentials, missing cookies, generic exceptions)
- ✓ Header construction with/without tokens
- ✓ Singleton pattern verification
- ✓ Successful query execution

### 2. list_namespaces Tests (3 tests)
- ✓ Successful namespace listing with counts
- ✓ Empty namespace handling
- ✓ Error handling

### 3. search_nodes Tests (4 tests)
- ✓ Basic search with results
- ✓ Search with filters (type, namespace, limit)
- ✓ No results found
- ✓ Error handling

### 4. get_node_details Tests (3 tests)
- ✓ Successful node detail retrieval
- ✓ Node not found handling
- ✓ Error handling

### 5. get_common_dimensions Tests (3 tests)
- ✓ Finding common dimensions across metrics
- ✓ No common dimensions found
- ✓ Error handling

### 6. build_metric_sql Tests (3 tests)
- ✓ Successful SQL generation with all parameters
- ✓ HTTP error handling (400, 500, etc.)
- ✓ Generic error handling

### 7. get_metric_data Tests (8 tests)
- ✓ Successful data retrieval
- ✓ Large result sets (>10 rows, truncation)
- ✓ Empty result sets
- ✓ Results with errors in response
- ✓ HTTP error handling
- ✓ Generic error handling

## Running Tests

```bash
# Run all MCP tests
pdm run pytest tests/mcp/test_tools.py -v

# Run with coverage report
pdm run pytest tests/mcp/ --cov=datajunction_server.mcp.tools --cov-report=term-missing -v

# Run specific test
pdm run pytest tests/mcp/test_tools.py::test_search_nodes_success -v
```

## Test Coverage Details

### Covered Functionality
- ✓ All 6 MCP tools (list_namespaces, search_nodes, get_node_details, get_common_dimensions, build_metric_sql, get_metric_data)
- ✓ Authentication mechanisms (API token, username/password, no credentials)
- ✓ GraphQL query execution
- ✓ REST API calls (SQL generation, data retrieval)
- ✓ Error handling for all tools
- ✓ Edge cases (empty results, missing data, malformed responses)
- ✓ HTTP errors (4xx, 5xx)
- ✓ Connection errors
- ✓ Response formatting

### Not Covered (2%)
The remaining 2% consists of minor defensive code paths that are difficult to trigger in testing:
- Specific branch conditions in SQL builder
- Edge cases in data retrieval responses

These uncovered paths represent defensive programming and do not affect normal operation.

## Notes

- All tests use mocking to avoid external dependencies
- Tests are async-aware using pytest-asyncio
- Error messages are verified for user-friendliness
- Both success and failure paths are tested for each tool
