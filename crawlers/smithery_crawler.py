#!/usr/bin/env python3
"""
Crawler for Smithery Registry (registry.smithery.ai).

- API key: Bearer token
- Pagination: page-based (?page=N), fixed 10 servers/page
- ~3,537 servers (354 pages)
- Returns: qualifiedName, displayName, description, useCount, verified, isDeployed, createdAt
- Note: homepage field points to Smithery URL, not actual project homepage
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

SMITHERY_URL = "https://registry.smithery.ai/servers"
SMITHERY_API_KEY = os.environ.get("SMITHERY_API_KEY", "")
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


def fetch_smithery_page(page=1, retries=3):
    """Fetch one page from Smithery. Returns 10 servers per page."""
    url = f"{SMITHERY_URL}?page={page}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {SMITHERY_API_KEY}",
        "User-Agent": "MCP-Atlas/1.0",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req, timeout=30)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 403 and attempt < retries - 1:
                wait = (attempt + 1) * 5
                log.warning("403 on page %d, retrying in %ds (attempt %d/%d)", page, wait, attempt + 1, retries)
                time.sleep(wait)
            else:
                body = e.read().decode() if e.fp else ""
                log.error("Smithery HTTP %d on page %d: %s", e.code, page, body[:200])
                raise


def normalize_github_url(url):
    """Normalize GitHub URL for dedup matching."""
    if not url:
        return ""
    url = url.rstrip("/").rstrip(".git")
    if "github.com/" in url:
        parts = url.split("github.com/")[1].split("/")
        if len(parts) >= 2:
            return f"https://github.com/{parts[0]}/{parts[1]}".lower()
    return ""


def parse_server(raw):
    """Parse a Smithery server entry into our schema shape."""
    qname = raw.get("qualifiedName", "")
    display = raw.get("displayName", "") or qname
    desc = raw.get("description", "")
    use_count = raw.get("useCount", 0) or 0
    created = raw.get("createdAt", "")
    verified = raw.get("verified", False)
    is_deployed = raw.get("isDeployed", False)

    # Smithery qualifiedName format: "owner/repo" or just "name"
    # Use as-is for the @id name field, prefixed with "smithery/" for uniqueness
    name = f"smithery/{qname}"

    return {
        "name": name,
        "displayName": display,
        "description": desc,
        "useCount": use_count,
        "createdAt": created if created else None,
        "listings": [{
            "registry": "smithery",
            "registryId": qname,
            "registryUrl": f"https://smithery.ai/server/{qname}",
            "verified": verified,
            "isDeployed": is_deployed,
            "lastCrawled": datetime.now(timezone.utc).isoformat(),
        }],
    }


def get_existing_servers(jwt):
    """Get all existing server names and GitHub URLs for dedup."""
    all_servers = []
    offset = 0
    while True:
        result = dgraph_gql(jwt, """
            query S($first: Int!, $offset: Int!) {
                queryServer(first: $first, offset: $offset) { name githubUrl displayName }
            }
        """, {"first": 500, "offset": offset})
        batch = result.get("data", {}).get("queryServer", [])
        all_servers.extend(batch)
        if len(batch) < 500:
            break
        offset += 500

    # Build lookups
    name_set = {s["name"] for s in all_servers}
    display_lookup = {}
    for s in all_servers:
        if s.get("displayName"):
            display_lookup[s["displayName"].lower().strip()] = s["name"]
    return name_set, display_lookup


def upsert_batch(jwt, servers):
    """Upsert servers into DGraph."""
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
    existing_names, display_lookup = get_existing_servers(jwt)
    log.info("Existing: %d servers, %d display names", len(existing_names), len(display_lookup))

    # Get total pages from first request
    first_page = fetch_smithery_page(page=1)
    pagination = first_page.get("pagination", {})
    total_pages = pagination.get("totalPages", 1)
    total_count = pagination.get("totalCount", 0)
    log.info("Smithery: %d servers across %d pages", total_count, total_pages)

    total_fetched = 0
    total_new = 0
    total_enriched = 0
    all_pages_data = [first_page]  # already have page 1

    # Process page 1
    for raw in first_page.get("servers", []):
        total_fetched += 1

    # Fetch remaining pages
    for page_num in range(2, total_pages + 1):
        try:
            data = fetch_smithery_page(page=page_num)
            all_pages_data.append(data)
            total_fetched += len(data.get("servers", []))
        except Exception as e:
            log.error("Failed page %d: %s", page_num, e)

        if page_num % 50 == 0:
            log.info("  Fetched page %d/%d (%d servers so far)", page_num, total_pages, total_fetched)

        # Rate limit: 1 second between pages
        time.sleep(1.0)

    log.info("Fetched all %d pages, %d total servers", len(all_pages_data), total_fetched)

    # Parse and upsert
    new_servers = []
    enriched_count = 0

    for page_data in all_pages_data:
        for raw in page_data.get("servers", []):
            parsed = parse_server(raw)
            name = parsed["name"]

            # Check if server with same Smithery name already exists
            if name in existing_names:
                # Update useCount + listing on existing
                listing = parsed.get("listings", [{}])[0]
                update = {"useCount": parsed.get("useCount", 0), "listings": [listing]}
                dgraph_gql(jwt, """
                    mutation U($patch: UpdateServerInput!) {
                        updateServer(input: $patch) { numUids }
                    }
                """, {"patch": {"filter": {"name": {"eq": name}}, "set": update}})
                enriched_count += 1
            else:
                new_servers.append(parsed)

    log.info("Parsed: %d new, %d existing to enrich", len(new_servers), enriched_count)

    # Upsert new servers in batches
    for i in range(0, len(new_servers), 50):
        batch = new_servers[i:i + 50]
        n = upsert_batch(jwt, batch)
        total_new += n
        if (i + 50) % 500 == 0:
            log.info("  Upserted %d/%d new servers", i + 50, len(new_servers))

    log.info("=== SMITHERY CRAWL COMPLETE ===")
    log.info("Servers fetched: %d", total_fetched)
    log.info("New servers added: %d", total_new)
    log.info("Existing servers enriched: %d", enriched_count)


if __name__ == "__main__":
    crawl()
