#!/usr/bin/env python3
"""
PyPI Enricher — fetches download counts and metadata for servers with PyPI packages.

Uses pypistats.org API for download counts and PyPI JSON API for metadata.
"""

import json
import logging
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
GROOT_PASSWORD = os.environ.get("DGRAPH_GROOT_PASSWORD", "")


def dgraph_login():
    payload = json.dumps({
        "query": 'mutation { login(userId: "groot", password: "' + GROOT_PASSWORD + '") { response { accessJWT } } }'
    }).encode()
    req = urllib.request.Request(f"{DGRAPH_URL}/admin", data=payload, headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())["data"]["login"]["response"]["accessJWT"]


def dgraph_query(jwt, query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql", data=payload,
        headers={"Content-Type": "application/json", "X-Dgraph-AccessToken": jwt},
    )
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def fetch_pypi_metadata(package_name):
    """Fetch package metadata from PyPI."""
    url = f"https://pypi.org/pypi/{urllib.request.quote(package_name)}/json"
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=15)
        data = json.loads(resp.read())
        info = data.get("info", {})
        return {
            "version": info.get("version", ""),
            "license": info.get("license", ""),
            "homepage": info.get("home_page") or info.get("project_url", ""),
            "description": info.get("summary", ""),
        }
    except Exception:
        return None


def fetch_pypi_downloads(package_name):
    """Fetch recent download stats from pypistats.org."""
    url = f"https://pypistats.org/api/packages/{urllib.request.quote(package_name)}/recent"
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=10)
        data = json.loads(resp.read())
        d = data.get("data", {})
        return {
            "weekly": d.get("last_week", 0),
            "monthly": d.get("last_month", 0),
        }
    except Exception:
        return {"weekly": 0, "monthly": 0}


def enrich_one(package_name):
    """Fetch all PyPI data for one package."""
    meta = fetch_pypi_metadata(package_name)
    downloads = fetch_pypi_downloads(package_name)
    return {"meta": meta, "downloads": downloads}


def main():
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()

    # Get all Python servers
    log.info("Querying Python servers...")
    result = dgraph_query(jwt, """
        { queryServer(filter: { language: { eq: "Python" } }, first: 1000) {
            name pypiPackage downloadsWeekly
        } }
    """)
    servers = result.get("data", {}).get("queryServer", [])
    servers_with_pypi = [s for s in servers if s.get("pypiPackage")]
    log.info("Found %d Python servers, %d with PyPI packages", len(servers), len(servers_with_pypi))

    if not servers_with_pypi:
        log.info("No servers to enrich.")
        return

    # Fetch in parallel (5 workers — pypistats rate limits more aggressively)
    enriched = []
    errors = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_server = {
            executor.submit(enrich_one, s["pypiPackage"]): s
            for s in servers_with_pypi
        }
        for i, future in enumerate(as_completed(future_to_server)):
            server = future_to_server[future]
            try:
                data = future.result()
                enriched.append((server, data))
            except Exception as e:
                errors += 1
                log.warning("Failed to enrich %s: %s", server["pypiPackage"], e)

            if (i + 1) % 50 == 0:
                log.info("  Fetched %d/%d PyPI packages...", i + 1, len(servers_with_pypi))

    log.info("Enriched %d packages (%d errors)", len(enriched), errors)

    # Update DGraph
    updated = 0
    for server, data in enriched:
        set_data = {}
        downloads = data.get("downloads", {})
        if downloads.get("weekly"):
            set_data["downloadsWeekly"] = downloads["weekly"]
        if downloads.get("monthly"):
            set_data["downloadsMonthly"] = downloads["monthly"]
        if data.get("meta"):
            meta = data["meta"]
            if meta.get("license") and len(meta["license"]) < 50:
                set_data["license"] = meta["license"]
            if meta.get("version"):
                set_data["version"] = meta["version"]

        if set_data:
            result = dgraph_query(jwt, """
                mutation UpdateServer($patch: UpdateServerInput!) {
                    updateServer(input: $patch) { numUids }
                }
            """, {"patch": {"filter": {"name": {"eq": server["name"]}}, "set": set_data}})
            if not result.get("errors"):
                updated += 1

        if updated % 50 == 0 and updated > 0:
            log.info("  Updated %d servers...", updated)

    log.info("=== PYPI ENRICHMENT COMPLETE ===")
    log.info("Servers enriched: %d/%d", updated, len(enriched))


if __name__ == "__main__":
    main()
