#!/usr/bin/env python3
"""
npm Enricher — fetches download counts and metadata for servers with npm packages.

Enriches existing DGraph Server records with:
- downloadsWeekly, downloadsMonthly
- homepage (from npm)
- license (from npm)
- version (latest from npm)
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
NPM_REGISTRY = "https://registry.npmjs.org"
NPM_DOWNLOADS = "https://api.npmjs.org/downloads/point"


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


def fetch_npm_metadata(package_name):
    """Fetch package metadata from npm registry."""
    url = f"{NPM_REGISTRY}/{urllib.request.quote(package_name, safe='@/')}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
        latest = data.get("dist-tags", {}).get("latest", "")
        latest_info = data.get("versions", {}).get(latest, {})
        return {
            "version": latest,
            "license": data.get("license") if isinstance(data.get("license"), str) else "",
            "homepage": data.get("homepage", ""),
            "description": data.get("description", ""),
            "repository": (data.get("repository", {}) or {}).get("url", "") if isinstance(data.get("repository"), dict) else "",
        }
    except Exception as e:
        return None


def fetch_npm_downloads(package_name, period="last-week"):
    """Fetch download counts from npm."""
    url = f"{NPM_DOWNLOADS}/{period}/{urllib.request.quote(package_name, safe='@/')}"
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=10)
        data = json.loads(resp.read())
        return data.get("downloads", 0)
    except Exception:
        return 0


def enrich_one(package_name):
    """Fetch all npm data for one package."""
    meta = fetch_npm_metadata(package_name)
    weekly = fetch_npm_downloads(package_name, "last-week")
    monthly = fetch_npm_downloads(package_name, "last-month")
    return {
        "npmPackage": package_name,
        "meta": meta,
        "downloadsWeekly": weekly,
        "downloadsMonthly": monthly,
    }


def main():
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()

    # Get all servers with npm packages (language=TypeScript implies npm)
    log.info("Querying servers with npm packages...")
    result = dgraph_query(jwt, """
        { queryServer(filter: { language: { eq: "TypeScript" } }, first: 1000) {
            name npmPackage downloadsWeekly
        } }
    """)
    servers = result.get("data", {}).get("queryServer", [])
    log.info("Found %d servers with npm packages", len(servers))

    if not servers:
        log.info("No servers to enrich.")
        return

    # Fetch npm data in parallel (10 workers, be polite)
    enriched = []
    errors = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_pkg = {
            executor.submit(enrich_one, s["npmPackage"]): s
            for s in servers if s.get("npmPackage")
        }
        for i, future in enumerate(as_completed(future_to_pkg)):
            server = future_to_pkg[future]
            try:
                data = future.result()
                enriched.append((server, data))
            except Exception as e:
                errors += 1
                log.warning("Failed to enrich %s: %s", server["npmPackage"], e)

            if (i + 1) % 100 == 0:
                log.info("  Fetched %d/%d npm packages...", i + 1, len(servers))

    log.info("Enriched %d packages (%d errors)", len(enriched), errors)

    # Update DGraph in batches
    updated = 0
    batch = []
    for server, data in enriched:
        update = {"filter": {"name": {"eq": server["name"]}}, "set": {}}
        if data["downloadsWeekly"]:
            update["set"]["downloadsWeekly"] = data["downloadsWeekly"]
        if data["downloadsMonthly"]:
            update["set"]["downloadsMonthly"] = data["downloadsMonthly"]
        if data["meta"]:
            if data["meta"].get("license"):
                update["set"]["license"] = data["meta"]["license"]
            if data["meta"].get("homepage") and not server.get("homepage"):
                update["set"]["homepage"] = data["meta"]["homepage"]
            if data["meta"].get("version"):
                update["set"]["version"] = data["meta"]["version"]

        if update["set"]:
            batch.append((server["name"], update["set"]))

    # Execute updates one by one (DGraph updateServer takes filter + set)
    for name, set_data in batch:
        mutation = """
        mutation UpdateServer($patch: UpdateServerInput!) {
            updateServer(input: $patch) { numUids }
        }
        """
        variables = {
            "patch": {
                "filter": {"name": {"eq": name}},
                "set": set_data,
            }
        }
        result = dgraph_query(jwt, mutation, variables)
        if result.get("errors"):
            log.warning("Failed to update %s: %s", name, result["errors"][0]["message"])
        else:
            updated += 1

        if updated % 100 == 0 and updated > 0:
            log.info("  Updated %d/%d servers...", updated, len(batch))

    log.info("=== ENRICHMENT COMPLETE ===")
    log.info("Servers enriched: %d/%d", updated, len(batch))

    # Show top 10 by downloads
    top = dgraph_query(jwt, """
        { queryServer(first: 10, order: { desc: downloadsWeekly }) {
            name npmPackage downloadsWeekly downloadsMonthly
        } }
    """)
    log.info("Top 10 by weekly downloads:")
    for s in top.get("data", {}).get("queryServer", []):
        log.info("  %s: %s weekly / %s monthly",
                 s["npmPackage"], f"{s['downloadsWeekly']:,}", f"{s['downloadsMonthly']:,}")


if __name__ == "__main__":
    main()
