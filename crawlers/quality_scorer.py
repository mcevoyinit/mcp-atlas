#!/usr/bin/env python3
"""
Quality Score Calculator — computes a 0-1 quality score for every server.

Formula:
    qualityScore = (
        0.25 * normalize(stars) +
        0.25 * normalize(downloadsWeekly) +
        0.20 * recency(lastCommit) +
        0.15 * hasDescription +
        0.15 * toolCompleteness
    )

Runs after crawling + enrichment, updates qualityScore field in DGraph.
"""

import json
import logging
import math
import os
import sys
import urllib.request
from datetime import datetime, timezone

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


def log_normalize(value, max_val):
    """Log-normalized score: maps large ranges to 0-1 using log scale."""
    if not value or value <= 0:
        return 0.0
    if not max_val or max_val <= 0:
        return 0.0
    return min(1.0, math.log1p(value) / math.log1p(max_val))


def recency_score(last_commit_str):
    """Score based on how recently the repo was updated. 0-1 scale."""
    if not last_commit_str:
        return 0.0
    try:
        lc = datetime.fromisoformat(last_commit_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_ago = (now - lc).days
        if days_ago <= 7:
            return 1.0
        elif days_ago <= 30:
            return 0.9
        elif days_ago <= 90:
            return 0.7
        elif days_ago <= 180:
            return 0.5
        elif days_ago <= 365:
            return 0.3
        else:
            return 0.1
    except Exception:
        return 0.0


def main():
    log.info("Authenticating to DGraph...")
    jwt = dgraph_login()

    # Fetch all servers with relevant fields
    all_servers = []
    offset = 0
    page_size = 500
    while True:
        result = dgraph_query(jwt, """
            query Servers($first: Int!, $offset: Int!) {
                queryServer(first: $first, offset: $offset) {
                    name stars downloadsWeekly lastCommit description
                    hasToolsAggregate { count }
                }
            }
        """, {"first": page_size, "offset": offset})
        batch = result.get("data", {}).get("queryServer", [])
        all_servers.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    log.info("Loaded %d servers", len(all_servers))

    # Find max values for normalization
    max_stars = max((s.get("stars") or 0) for s in all_servers) if all_servers else 1
    max_downloads = max((s.get("downloadsWeekly") or 0) for s in all_servers) if all_servers else 1
    log.info("Max stars: %d, Max weekly downloads: %d", max_stars, max_downloads)

    # Calculate scores
    scores = []
    for s in all_servers:
        stars = s.get("stars") or 0
        downloads = s.get("downloadsWeekly") or 0
        last_commit = s.get("lastCommit") or ""
        has_desc = 1.0 if s.get("description") and len(s["description"]) > 20 else 0.0
        tool_count = (s.get("hasToolsAggregate") or {}).get("count", 0)
        has_tools = 1.0 if tool_count > 0 else 0.0

        score = (
            0.25 * log_normalize(stars, max_stars)
            + 0.25 * log_normalize(downloads, max_downloads)
            + 0.20 * recency_score(last_commit)
            + 0.15 * has_desc
            + 0.15 * has_tools
        )

        scores.append((s["name"], round(score, 4)))

    # Update DGraph
    updated = 0
    for i, (name, score) in enumerate(scores):
        mutation = """
        mutation UpdateServer($patch: UpdateServerInput!) {
            updateServer(input: $patch) { numUids }
        }
        """
        result = dgraph_query(jwt, mutation, {
            "patch": {"filter": {"name": {"eq": name}}, "set": {"qualityScore": score}}
        })
        if not result.get("errors"):
            updated += 1

        if (i + 1) % 200 == 0:
            log.info("  Scored %d/%d servers...", i + 1, len(scores))

    log.info("=== QUALITY SCORING COMPLETE ===")
    log.info("Servers scored: %d/%d", updated, len(scores))

    # Distribution
    buckets = {"0.0-0.1": 0, "0.1-0.2": 0, "0.2-0.3": 0, "0.3-0.4": 0,
               "0.4-0.5": 0, "0.5-0.6": 0, "0.6-0.7": 0, "0.7-0.8": 0,
               "0.8-0.9": 0, "0.9-1.0": 0}
    for _, score in scores:
        bucket = f"{int(score * 10) / 10:.1f}-{int(score * 10) / 10 + 0.1:.1f}"
        if bucket in buckets:
            buckets[bucket] += 1
    log.info("Score distribution:")
    for bucket, count in sorted(buckets.items()):
        bar = "#" * (count // 10)
        log.info("  %s: %4d %s", bucket, count, bar)

    # Top 15
    top = sorted(scores, key=lambda x: x[1], reverse=True)[:15]
    log.info("Top 15 by quality score:")
    for name, score in top:
        log.info("  %.3f  %s", score, name)


if __name__ == "__main__":
    main()
