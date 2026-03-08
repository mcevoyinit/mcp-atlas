#!/usr/bin/env python3
"""
Crawler for the Official MCP Registry (registry.modelcontextprotocol.io).

- No auth required
- Paginated via cursor
- ~2,880 servers
- Returns: name, description, version, repository, packages (npm/pypi/oci)
"""

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REGISTRY_URL = "https://registry.modelcontextprotocol.io/v0.1/servers"
PAGE_SIZE = 96  # max allowed
DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
GROOT_PASSWORD = os.environ.get("DGRAPH_GROOT_PASSWORD", "")


def dgraph_login():
    """Get JWT token from DGraph."""
    payload = json.dumps({
        "query": 'mutation { login(userId: "groot", password: "' + GROOT_PASSWORD + '") { response { accessJWT refreshJWT } } }'
    }).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/admin",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    result = json.loads(resp.read())
    return result["data"]["login"]["response"]["accessJWT"]


def dgraph_mutate(jwt, mutation, variables=None):
    """Execute a GraphQL mutation against DGraph."""
    payload = json.dumps({"query": mutation, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Dgraph-AccessToken": jwt,
        },
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def fetch_registry_page(cursor=None):
    """Fetch one page from the official MCP registry."""
    url = f"{REGISTRY_URL}?limit={PAGE_SIZE}&version=latest"
    if cursor:
        url += f"&cursor={cursor}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def parse_server(raw):
    """Parse a raw registry server entry into our schema shape."""
    server = raw.get("server", {})
    meta = raw.get("_meta", {}).get("io.modelcontextprotocol.registry/official", {})

    name = server.get("name", "")
    repo = server.get("repository", {})
    github_url = repo.get("url", "") if repo.get("source") == "github" else ""

    packages = server.get("packages", [])
    npm_pkg = ""
    pypi_pkg = ""
    transports = []
    for pkg in packages:
        reg_type = pkg.get("registryType", "")
        if reg_type == "npm":
            npm_pkg = pkg.get("identifier", "")
        elif reg_type == "pypi":
            pypi_pkg = pkg.get("identifier", "")
        transport = pkg.get("transport", {}).get("type", "")
        if transport and transport not in transports:
            transports.append(transport)

    # Detect language from packages
    language = ""
    if npm_pkg:
        language = "TypeScript"
    elif pypi_pkg:
        language = "Python"

    return {
        "name": name,
        "displayName": server.get("displayName", name.split("/")[-1] if "/" in name else name),
        "description": server.get("description", ""),
        "githubUrl": github_url,
        "npmPackage": npm_pkg,
        "pypiPackage": pypi_pkg,
        "homepage": server.get("homepage", ""),
        "language": language,
        "license": server.get("license", ""),
        "version": server.get("version", ""),
        "transport": transports,
        "createdAt": meta.get("publishedAt", ""),
        "listings": [{
            "registry": "official",
            "registryId": name,
            "registryUrl": f"https://registry.modelcontextprotocol.io/servers/{name}",
            "verified": meta.get("status") == "active",
            "lastCrawled": datetime.now(timezone.utc).isoformat(),
            "rawData": json.dumps(raw),
        }],
    }


def upsert_servers(jwt, servers):
    """Upsert a batch of servers into DGraph.

    Uses addServer with upsert: true (DGraph @id fields auto-upsert).
    """
    if not servers:
        return 0

    # Build the input — DGraph's @id on name handles upsert
    inputs = []
    for s in servers:
        entry = {k: v for k, v in s.items() if v and k != "listings"}
        # Listings need to be nested
        if s.get("listings"):
            entry["listings"] = s["listings"]
        inputs.append(entry)

    mutation = """
    mutation AddServers($input: [AddServerInput!]!) {
        addServer(input: $input, upsert: true) {
            numUids
        }
    }
    """
    result = dgraph_mutate(jwt, mutation, {"input": inputs})

    if result.get("errors"):
        for e in result["errors"]:
            log.error("DGraph error: %s", e["message"])
        return 0

    return result.get("data", {}).get("addServer", {}).get("numUids", 0)


def crawl():
    """Main crawl loop."""
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()
    log.info("Authenticated.")

    cursor = None
    total_fetched = 0
    total_upserted = 0
    page = 0

    while True:
        page += 1
        log.info("Fetching page %d (cursor=%s)...", page, cursor[:20] + "..." if cursor else "None")

        try:
            data = fetch_registry_page(cursor)
        except Exception as e:
            log.error("Failed to fetch page %d: %s", page, e)
            break

        raw_servers = data.get("servers", [])
        if not raw_servers:
            log.info("No more servers. Done.")
            break

        total_fetched += len(raw_servers)
        log.info("Got %d servers (total: %d)", len(raw_servers), total_fetched)

        # Parse
        parsed = []
        for raw in raw_servers:
            try:
                parsed.append(parse_server(raw))
            except Exception as e:
                log.warning("Failed to parse server: %s", e)

        # Upsert in batches of 50
        batch_size = 50
        for i in range(0, len(parsed), batch_size):
            batch = parsed[i : i + batch_size]
            upserted = upsert_servers(jwt, batch)
            total_upserted += upserted
            log.info("  Upserted batch: %d/%d", upserted, len(batch))

        # Next page
        next_cursor = data.get("metadata", {}).get("nextCursor")
        if not next_cursor or next_cursor == cursor:
            log.info("No next cursor. Done.")
            break
        cursor = next_cursor

        # Be polite
        time.sleep(0.5)

    log.info("=== CRAWL COMPLETE ===")
    log.info("Pages fetched: %d", page)
    log.info("Servers fetched: %d", total_fetched)
    log.info("Servers upserted: %d", total_upserted)
    return total_fetched, total_upserted


if __name__ == "__main__":
    fetched, upserted = crawl()
    sys.exit(0 if upserted > 0 else 1)
