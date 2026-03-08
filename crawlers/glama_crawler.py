#!/usr/bin/env python3
"""
Crawler for Glama MCP Registry (glama.ai/api/mcp/v1/servers).

- No auth required
- Cursor-based pagination (endCursor from pageInfo)
- ~17,000 servers (but heavy overlap with other registries)
- Returns: name, namespace, slug, description, repository, spdxLicense, tools, attributes
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

GLAMA_URL = "https://glama.ai/api/mcp/v1/servers"
PAGE_SIZE = 100
DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
GROOT_PASSWORD = os.environ.get("DGRAPH_GROOT_PASSWORD", "")


def dgraph_login():
    payload = json.dumps({
        "query": 'mutation { login(userId: "groot", password: "' + GROOT_PASSWORD + '") { response { accessJWT } } }'
    }).encode()
    req = urllib.request.Request(f"{DGRAPH_URL}/admin", data=payload, headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())["data"]["login"]["response"]["accessJWT"]


def dgraph_gql(jwt, query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql", data=payload,
        headers={"Content-Type": "application/json", "X-Dgraph-AccessToken": jwt},
    )
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def fetch_glama_page(cursor=None):
    """Fetch one page from Glama API."""
    url = f"{GLAMA_URL}?limit={PAGE_SIZE}"
    if cursor:
        url += f"&cursor={cursor}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def normalize_github_url(url):
    if not url:
        return ""
    url = url.rstrip("/").rstrip(".git")
    if "github.com/" in url:
        parts = url.split("github.com/")[1].split("/")
        if len(parts) >= 2:
            return f"https://github.com/{parts[0]}/{parts[1]}".lower()
    return ""


def parse_server(raw):
    """Parse a Glama server entry."""
    name = raw.get("name", "")
    namespace = raw.get("namespace", "")
    slug = raw.get("slug", "")
    desc = raw.get("description", "")
    repo = raw.get("repository", {}) or {}
    spdx = raw.get("spdxLicense")
    if isinstance(spdx, dict):
        license_id = spdx.get("name", "") or spdx.get("id", "")
    elif isinstance(spdx, list) and spdx and isinstance(spdx[0], dict):
        license_id = spdx[0].get("name", "") or spdx[0].get("id", "")
    else:
        license_id = ""
    tools = raw.get("tools", []) or []
    attrs_raw = raw.get("attributes", []) or []
    # attributes is a list of strings like ["hosting:hybrid", "author:official"]
    attrs = {}
    if isinstance(attrs_raw, list):
        for a in attrs_raw:
            if isinstance(a, str) and ":" in a:
                k, v = a.split(":", 1)
                attrs[k] = v
    elif isinstance(attrs_raw, dict):
        attrs = attrs_raw
    url = raw.get("url", "")

    # GitHub URL from repository
    github_url = ""
    repo_url = repo.get("url", "")
    if repo_url and "github.com" in repo_url:
        github_url = normalize_github_url(repo_url)

    # Server name for DGraph — use glama/ prefix for uniqueness
    full_name = f"{namespace}/{name}" if namespace else name
    dgraph_name = f"glama/{full_name}"

    # Detect language from attributes or tools
    language = ""
    if attrs.get("language"):
        language = attrs["language"]

    return {
        "name": dgraph_name,
        "displayName": name,
        "description": desc,
        "githubUrl": github_url,
        "language": language,
        "license": license_id,
        "homepage": url,
        "listings": [{
            "registry": "glama",
            "registryId": raw.get("id", full_name),
            "registryUrl": url,
            "lastCrawled": datetime.now(timezone.utc).isoformat(),
        }],
    }, github_url, len(tools)


def get_existing_github_urls(jwt):
    """Get all existing servers' GitHub URLs for dedup."""
    all_servers = []
    offset = 0
    while True:
        result = dgraph_gql(jwt, """
            query S($first: Int!, $offset: Int!) {
                queryServer(first: $first, offset: $offset) { name githubUrl }
            }
        """, {"first": 500, "offset": offset})
        batch = result.get("data", {}).get("queryServer", [])
        all_servers.extend(batch)
        if len(batch) < 500:
            break
        offset += 500

    lookup = {}
    for s in all_servers:
        if s.get("githubUrl"):
            norm = normalize_github_url(s["githubUrl"])
            if norm:
                lookup[norm] = s["name"]
    return lookup


def upsert_batch(jwt, servers):
    if not servers:
        return 0
    inputs = []
    for s in servers:
        entry = {k: v for k, v in s.items() if v is not None and v != "" and k != "listings"}
        if s.get("listings"):
            entry["listings"] = s["listings"]
        inputs.append(entry)

    result = dgraph_gql(jwt, """
        mutation Add($input: [AddServerInput!]!) {
            addServer(input: $input, upsert: true) { numUids }
        }
    """, {"input": inputs})

    if result.get("errors"):
        for e in result["errors"]:
            log.error("DGraph: %s", e["message"])
        return 0
    return result.get("data", {}).get("addServer", {}).get("numUids", 0)


def crawl():
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()

    log.info("Loading existing servers for dedup...")
    github_lookup = get_existing_github_urls(jwt)
    log.info("Loaded %d existing servers with GitHub URLs", len(github_lookup))

    cursor = None
    total_fetched = 0
    total_new = 0
    total_enriched = 0
    total_overlap = 0
    page = 0
    new_batch = []

    while True:
        page += 1
        log.info("Fetching page %d...", page)

        try:
            data = fetch_glama_page(cursor=cursor)
        except Exception as e:
            log.error("Failed page %d: %s", page, e)
            break

        servers = data.get("servers", [])
        if not servers:
            log.info("No more servers. Done.")
            break

        total_fetched += len(servers)

        for raw in servers:
            try:
                parsed, github_url, tool_count = parse_server(raw)
            except Exception as e:
                log.warning("Parse error: %s", e)
                continue

            norm = normalize_github_url(github_url)

            if norm and norm in github_lookup:
                # Already exists — add Glama listing
                existing_name = github_lookup[norm]
                listing = parsed.get("listings", [{}])[0]
                dgraph_gql(jwt, """
                    mutation U($patch: UpdateServerInput!) {
                        updateServer(input: $patch) { numUids }
                    }
                """, {"patch": {"filter": {"name": {"eq": existing_name}}, "set": {"listings": [listing]}}})
                total_overlap += 1
            else:
                new_batch.append(parsed)
                # Track for future dedup within this crawl
                if norm:
                    github_lookup[norm] = parsed["name"]

        # Upsert new servers every 200
        if len(new_batch) >= 200:
            n = upsert_batch(jwt, new_batch)
            total_new += n
            log.info("  Upserted %d new servers", n)
            new_batch = []

        if page % 20 == 0:
            log.info("  Progress: %d fetched, %d new, %d overlap", total_fetched, total_new, total_overlap)

        # Pagination
        page_info = data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            log.info("No next page. Done.")
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break

        # Rate limit
        time.sleep(0.5)

    # Final upsert
    if new_batch:
        n = upsert_batch(jwt, new_batch)
        total_new += n

    log.info("=== GLAMA CRAWL COMPLETE ===")
    log.info("Pages: %d", page)
    log.info("Servers fetched: %d", total_fetched)
    log.info("New servers added: %d", total_new)
    log.info("Overlap with existing: %d", total_overlap)


if __name__ == "__main__":
    crawl()
