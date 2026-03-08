#!/usr/bin/env python3
"""
GitHub Enricher — fetches stars, last commit, and repo metadata for servers with GitHub URLs.

Uses the GitHub REST API (unauthenticated: 60 req/hr, authenticated: 5000 req/hr).
Set GITHUB_TOKEN env var for higher rate limits.
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

DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
GROOT_PASSWORD = os.environ.get("DGRAPH_GROOT_PASSWORD", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")


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


def parse_github_url(url):
    """Extract owner/repo from a GitHub URL."""
    if not url:
        return None
    url = url.rstrip("/").rstrip(".git")
    parts = url.split("github.com/")
    if len(parts) < 2:
        return None
    path = parts[1].split("/")
    if len(path) >= 2:
        return f"{path[0]}/{path[1]}"
    return None


def fetch_github_repo(owner_repo):
    """Fetch repo metadata from GitHub API."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    url = f"https://api.github.com/repos/{owner_repo}"
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
        return {
            "stars": data.get("stargazers_count", 0),
            "lastCommit": data.get("pushed_at", ""),
            "license": (data.get("license") or {}).get("spdx_id", ""),
            "homepage": data.get("homepage", ""),
            "description": data.get("description", ""),
            "language": data.get("language", ""),
        }
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        if e.code == 403:
            # Rate limited — check headers
            remaining = e.headers.get("X-RateLimit-Remaining", "?")
            reset = e.headers.get("X-RateLimit-Reset", "0")
            log.warning("Rate limited (remaining=%s, reset=%s)", remaining, reset)
            return "RATE_LIMITED"
        raise
    except Exception as e:
        log.warning("GitHub API error for %s: %s", owner_repo, e)
        return None


def main():
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()

    # Get all servers — paginate with offset
    all_servers = []
    offset = 0
    page_size = 500
    while True:
        result = dgraph_query(jwt, """
            query Servers($first: Int!, $offset: Int!) {
                queryServer(first: $first, offset: $offset) {
                    name githubUrl stars
                }
            }
        """, {"first": page_size, "offset": offset})
        batch = result.get("data", {}).get("queryServer", [])
        all_servers.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    # Filter to those with GitHub URLs
    servers_with_github = [s for s in all_servers if s.get("githubUrl")]
    log.info("Found %d/%d servers with GitHub URLs", len(servers_with_github), len(all_servers))

    if not servers_with_github:
        log.info("No servers to enrich.")
        return

    # Check rate limit
    if GITHUB_TOKEN:
        log.info("Using authenticated GitHub API (5000 req/hr)")
    else:
        log.info("Using unauthenticated GitHub API (60 req/hr) — set GITHUB_TOKEN for more")
        if len(servers_with_github) > 55:
            log.warning("Too many servers for unauthenticated API. Processing first 55.")
            servers_with_github = servers_with_github[:55]

    updated = 0
    skipped = 0
    rate_limited = False

    for i, server in enumerate(servers_with_github):
        if rate_limited:
            break

        owner_repo = parse_github_url(server["githubUrl"])
        if not owner_repo:
            skipped += 1
            continue

        data = fetch_github_repo(owner_repo)
        if data == "RATE_LIMITED":
            log.warning("Rate limited at server %d/%d. Stopping.", i + 1, len(servers_with_github))
            rate_limited = True
            break
        if data is None:
            skipped += 1
            continue

        # Update DGraph
        set_data = {}
        if data["stars"]:
            set_data["stars"] = data["stars"]
        if data["lastCommit"]:
            set_data["lastCommit"] = data["lastCommit"]
        if data["license"] and data["license"] != "NOASSERTION":
            set_data["license"] = data["license"]

        if set_data:
            mutation = """
            mutation UpdateServer($patch: UpdateServerInput!) {
                updateServer(input: $patch) { numUids }
            }
            """
            result = dgraph_query(jwt, mutation, {
                "patch": {"filter": {"name": {"eq": server["name"]}}, "set": set_data}
            })
            if not result.get("errors"):
                updated += 1

        if (i + 1) % 50 == 0:
            log.info("  Processed %d/%d (updated: %d, skipped: %d)",
                     i + 1, len(servers_with_github), updated, skipped)

        # Rate limit: unauthenticated = 1/sec, authenticated = 0.8/sec
        time.sleep(0.8 if GITHUB_TOKEN else 1.1)

    log.info("=== GITHUB ENRICHMENT COMPLETE ===")
    log.info("Processed: %d, Updated: %d, Skipped: %d", i + 1, updated, skipped)

    # Show top by stars
    top = dgraph_query(jwt, """
        { queryServer(first: 10, order: { desc: stars }) {
            name stars githubUrl lastCommit
        } }
    """)
    log.info("Top 10 by stars:")
    for s in top.get("data", {}).get("queryServer", []):
        stars = s.get("stars") or 0
        lc = (s.get("lastCommit") or "")[:10]
        log.info("  ★%5d  %s  (%s)", stars, s["name"], lc)


if __name__ == "__main__":
    main()
