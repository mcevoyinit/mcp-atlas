#!/usr/bin/env python3
"""Entry point for the MCP Catalog discovery server."""

from server import mcp

if __name__ == "__main__":
    mcp.run(transport="stdio")
