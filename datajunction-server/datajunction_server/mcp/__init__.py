"""
DataJunction MCP (Model Context Protocol) server.

Exposes DJ semantic-layer tools to AI agents over HTTP, mounted as a
sub-app on the main FastAPI application. Tools call internal services
directly (no HTTP loop-through to DJ's own REST API).
"""
