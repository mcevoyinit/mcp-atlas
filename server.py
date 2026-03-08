#!/usr/bin/env python3
"""
MCP Atlas — Discovery Server

An MCP server that helps AI agents find the right MCP tools.
Backed by a DGraph database of 2,136 categorized, quality-scored MCP servers.

Tools:
  - search_servers: Find MCP servers by keyword, category, or language
  - get_server_details: Get full details about a specific server
  - browse_categories: List all categories with server counts
  - recommend_servers: Get contextual recommendations for an agent's current task
"""

import json
import os
import urllib.request
from typing import Optional

from mcp.server.fastmcp import FastMCP

DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")

mcp = FastMCP(
    "MCP Atlas",
    instructions=(
        "You are a tool discovery assistant. Help agents find the right MCP servers "
        "for their tasks. Use search_servers for keyword queries, browse_categories "
        "to explore what's available, get_server_details for deep info on a specific "
        "server, and recommend_servers when you know what the agent is trying to do."
    ),
)


def _gql(query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query against DGraph."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except urllib.error.URLError as e:
        return {"errors": [{"message": f"DGraph unavailable: {e}"}], "data": {}}
    except Exception as e:
        return {"errors": [{"message": f"Query failed: {e}"}], "data": {}}


def _format_server(s: dict, verbose: bool = False) -> dict:
    """Format a server record for output."""
    result = {
        "name": s.get("displayName") or s.get("name", ""),
        "id": s.get("name", ""),
        "description": s.get("description", ""),
        "qualityScore": round(s.get("qualityScore") or 0, 3),
    }
    if s.get("stars"):
        result["stars"] = s["stars"]
    if s.get("language"):
        result["language"] = s["language"]
    if s.get("githubUrl"):
        result["githubUrl"] = s["githubUrl"]
    if s.get("inCategories"):
        result["categories"] = [c["name"] for c in s["inCategories"]]

    if verbose:
        if s.get("npmPackage"):
            result["npmPackage"] = s["npmPackage"]
        if s.get("pypiPackage"):
            result["pypiPackage"] = s["pypiPackage"]
        if s.get("homepage"):
            result["homepage"] = s["homepage"]
        if s.get("license"):
            result["license"] = s["license"]
        if s.get("version"):
            result["version"] = s["version"]
        if s.get("downloadsWeekly"):
            result["downloadsWeekly"] = s["downloadsWeekly"]
        if s.get("transport"):
            result["transport"] = s["transport"]
        if s.get("hasTools"):
            result["tools"] = [
                {"name": t["name"], "description": t.get("description", "")}
                for t in s["hasTools"]
            ]
        if s.get("listings"):
            result["availableOn"] = list(
                {lst["registry"] for lst in s["listings"] if lst.get("registry")}
            )

    return result


SERVER_FIELDS = """
    name displayName description githubUrl language license
    stars downloadsWeekly qualityScore
    inCategories { name }
"""

SERVER_FIELDS_FULL = """
    name displayName description githubUrl npmPackage pypiPackage
    homepage language license version transport
    stars downloadsWeekly downloadsMonthly useCount
    lastCommit createdAt qualityScore bestFor pricing
    inCategories { name }
    compatibleWith { name }
    hasTools { name title description }
    listings { registry registryId registryUrl }
"""


def _sanitize(text: str) -> str:
    """Sanitize user input for safe inclusion in GraphQL string literals."""
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").strip()


@mcp.tool()
def search_servers(
    query: str,
    category: Optional[str] = None,
    language: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search for MCP servers by keyword, category, or programming language.

    Args:
        query: Search terms (matched against server name and description)
        category: Filter by category (e.g. "Database & Storage", "AI & LLM")
        language: Filter by language (e.g. "TypeScript", "Python")
        limit: Maximum results to return (default 10, max 50)
    """
    limit = min(max(limit, 1), 50)
    safe_query = _sanitize(query) if query else ""
    safe_lang = _sanitize(language) if language else ""

    # Build filter
    filters = []
    if safe_query:
        filters.append(f'description: {{ alloftext: "{safe_query}" }}')
    if safe_lang:
        filters.append(f'language: {{ eq: "{safe_lang}" }}')

    filter_str = ""
    if filters:
        filter_str = f"filter: {{ {', '.join(filters)} }}"

    # If category specified, query through category → servers with server-side filtering
    if category:
        nested_filters = []
        if safe_query:
            nested_filters.append(f'description: {{ anyoftext: "{safe_query}" }}')
        if safe_lang:
            nested_filters.append(f'language: {{ eq: "{safe_lang}" }}')
        nested_filter = ""
        if nested_filters:
            nested_filter = f"filter: {{ {', '.join(nested_filters)} }},"

        r = _gql(
            f"""query C($cat: String!, $limit: Int!) {{
                getCategory(name: $cat) {{
                    servers({nested_filter} first: $limit, order: {{ desc: qualityScore }}) {{
                        {SERVER_FIELDS}
                    }}
                }}
            }}""",
            {"cat": category, "limit": limit},
        )
        servers = (r.get("data", {}).get("getCategory") or {}).get("servers", [])
    else:
        r = _gql(
            f"""query S($limit: Int!) {{
                queryServer({filter_str}, first: $limit, order: {{ desc: qualityScore }}) {{
                    {SERVER_FIELDS}
                }}
            }}""",
            {"limit": limit},
        )
        servers = r.get("data", {}).get("queryServer", [])

    return json.dumps([_format_server(s) for s in servers])


@mcp.tool()
def get_server_details(server_id: str) -> str:
    """Get full details about a specific MCP server.

    Args:
        server_id: The server identifier (e.g. "io.github.modelcontextprotocol/server-filesystem")
    """
    r = _gql(
        f"""query G($name: String!) {{
            getServer(name: $name) {{
                {SERVER_FIELDS_FULL}
            }}
        }}""",
        {"name": server_id},
    )
    server = r.get("data", {}).get("getServer")
    if not server:
        return json.dumps({"error": f"Server '{server_id}' not found"})
    return json.dumps(_format_server(server, verbose=True))


@mcp.tool()
def browse_categories() -> str:
    """List all MCP server categories with descriptions and server counts.
    Use this to understand what types of tools are available."""
    r = _gql("""{ queryCategory(order: { desc: displayOrder }) {
        name slug description servers { name }
    } }""")
    categories = r.get("data", {}).get("queryCategory", [])
    return json.dumps([
        {
            "name": c["name"],
            "description": c.get("description", ""),
            "serverCount": len(c.get("servers", [])),
        }
        for c in sorted(categories, key=lambda c: len(c.get("servers", [])), reverse=True)
    ])


@mcp.tool()
def recommend_servers(
    task_description: str,
    limit: int = 5,
) -> str:
    """Get MCP server recommendations based on what you're trying to accomplish.

    Describe the task or goal, and this tool returns the most relevant MCP servers
    ranked by quality and relevance.

    Args:
        task_description: What you're trying to do (e.g. "Set up a PostgreSQL database
            with schema management" or "Send Slack notifications from my CI pipeline")
        limit: Number of recommendations (default 5, max 20)
    """
    limit = min(max(limit, 1), 20)
    safe_desc = _sanitize(task_description)

    # Try fulltext search with the full description first (AND matching)
    r = _gql(
        f"""query R($limit: Int!) {{
            queryServer(
                filter: {{ description: {{ alloftext: "{safe_desc}" }} }},
                first: $limit,
                order: {{ desc: qualityScore }}
            ) {{
                {SERVER_FIELDS}
            }}
        }}""",
        {"limit": limit},
    )
    servers = r.get("data", {}).get("queryServer", [])

    # If not enough results, try with anyoftext (OR matching)
    if len(servers) < limit:
        r2 = _gql(
            f"""query R($limit: Int!) {{
                queryServer(
                    filter: {{ description: {{ anyoftext: "{safe_desc}" }} }},
                    first: $limit,
                    order: {{ desc: qualityScore }}
                ) {{
                    {SERVER_FIELDS}
                }}
            }}""",
            {"limit": limit * 2},
        )
        existing_names = {s["name"] for s in servers}
        for s in r2.get("data", {}).get("queryServer", []):
            if s["name"] not in existing_names and len(servers) < limit:
                servers.append(s)
                existing_names.add(s["name"])

    results = [_format_server(s) for s in servers]

    if not results:
        return json.dumps([{"message": "No matching servers found. Try broader search terms or browse_categories to explore."}])

    return json.dumps(results)


if __name__ == "__main__":
    mcp.run(transport="stdio")
