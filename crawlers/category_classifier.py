#!/usr/bin/env python3
"""
Classify MCP servers into categories using an AI classifier.

Reads servers from DGraph, classifies in batches of 20,
updates DGraph with category assignments.

Uses: claude -p --model haiku --tools "" --disable-slash-commands
"""

import json
import logging
import os
import subprocess
import sys
import time
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
BATCH_SIZE = 20
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

CATEGORIES = [
    "Monitoring & Observability",
    "Browser & Web",
    "Database & Storage",
    "Authentication & Identity",
    "Search & Discovery",
    "Data & Analytics",
    "Cloud Infrastructure",
    "Communication & Chat",
    "Email & Messaging",
    "File System & Documents",
    "DevOps & CI/CD",
    "CMS & Content",
    "Git & Version Control",
    "Payments & Billing",
    "AI & LLM",
]

SYSTEM_PROMPT = """You are a classifier for MCP (Model Context Protocol) servers. Given a list of servers with their names and descriptions, assign each server to 1-3 categories from this exact list:

CATEGORIES:
- Monitoring & Observability
- Browser & Web
- Database & Storage
- Authentication & Identity
- Search & Discovery
- Data & Analytics
- Cloud Infrastructure
- Communication & Chat
- Email & Messaging
- File System & Documents
- DevOps & CI/CD
- CMS & Content
- Git & Version Control
- Payments & Billing
- AI & LLM

RULES:
1. Assign 1-3 categories per server. Prefer 1-2; only use 3 if the server genuinely spans 3 domains.
2. Use EXACT category names from the list above.
3. If a server doesn't fit any category well, pick the closest match. Every server must get at least 1 category.
4. For generic/vague descriptions, infer from the server name.
5. Output ONLY valid JSON — no markdown, no explanation.

OUTPUT FORMAT (strict JSON array):
[
  {"name": "server-name-1", "categories": ["Category A", "Category B"]},
  {"name": "server-name-2", "categories": ["Category C"]}
]"""


def gql(query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql", data=payload,
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=60).read())


def classify_batch(servers):
    """Classify a batch of servers using Claude CLI."""
    # Build the prompt
    lines = []
    for s in servers:
        name = s.get("displayName") or s.get("name", "")
        desc = (s.get("description") or "")[:200]
        lang = s.get("language") or ""
        lines.append(f"- {s['name']}: {name} — {desc}" + (f" [{lang}]" if lang else ""))

    prompt = "Classify these MCP servers:\n\n" + "\n".join(lines)

    # Call Claude CLI
    result = subprocess.run(
        ["claude", "-p", "--model", "haiku", "--tools", "", "--disable-slash-commands"],
        input=f"{SYSTEM_PROMPT}\n\n{prompt}",
        capture_output=True, text=True, timeout=60,
    )

    if result.returncode != 0:
        log.error("Claude CLI error: %s", result.stderr[:200])
        return None

    # Parse JSON from response
    output = result.stdout.strip()
    # Handle potential markdown wrapping
    if "```" in output:
        output = output.split("```")[1]
        if output.startswith("json"):
            output = output[4:]
        output = output.strip()

    try:
        return json.loads(output)
    except json.JSONDecodeError:
        # Try to find JSON array in the output
        start = output.find("[")
        end = output.rfind("]") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(output[start:end])
            except json.JSONDecodeError:
                pass
        log.error("Failed to parse JSON: %s", output[:300])
        return None


def update_categories(classifications):
    """Update DGraph with category assignments."""
    updated = 0
    for item in classifications:
        name = item.get("name", "")
        cats = item.get("categories", [])
        if not name or not cats:
            continue

        # Validate categories
        valid_cats = [c for c in cats if c in CATEGORIES]
        if not valid_cats:
            log.warning("No valid categories for %s: %s", name, cats)
            continue

        # Update server with category references
        cat_refs = [{"name": c} for c in valid_cats]
        r = gql("""
            mutation U($patch: UpdateServerInput!) {
                updateServer(input: $patch) { numUids }
            }
        """, {"patch": {"filter": {"name": {"eq": name}}, "set": {"inCategories": cat_refs}}})

        if r.get("errors"):
            log.warning("Update error for %s: %s", name, r["errors"][0]["message"][:100])
        else:
            updated += 1

    return updated


def main():
    # Load all servers
    log.info("Loading servers from DGraph...")
    all_servers = []
    offset = 0
    while True:
        r = gql("""query S($first: Int!, $offset: Int!) {
            queryServer(first: $first, offset: $offset) {
                name displayName description language
                inCategories { name }
            }
        }""", {"first": 500, "offset": offset})
        batch = r.get("data", {}).get("queryServer", [])
        all_servers.extend(batch)
        if len(batch) < 500:
            break
        offset += 500

    # Filter to unclassified servers only
    unclassified = [s for s in all_servers if not s.get("inCategories")]
    log.info("Total servers: %d, Unclassified: %d", len(all_servers), len(unclassified))

    if not unclassified:
        log.info("All servers already classified!")
        return

    # Process in batches
    total_classified = 0
    total_batches = (len(unclassified) + BATCH_SIZE - 1) // BATCH_SIZE

    # Save progress file for resumability
    progress_file = os.path.join(DATA_DIR, "classification_progress.json")
    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)
        log.info("Resuming from %d previously classified", len(progress))

    # Filter out already-progressed
    remaining = [s for s in unclassified if s["name"] not in progress]
    log.info("Remaining to classify: %d", len(remaining))

    for i in range(0, len(remaining), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = remaining[i:i + BATCH_SIZE]

        log.info("Batch %d/%d (%d servers)...", batch_num, total_batches, len(batch))

        classifications = classify_batch(batch)
        if not classifications:
            log.error("Batch %d failed, retrying in 5s...", batch_num)
            time.sleep(5)
            classifications = classify_batch(batch)
            if not classifications:
                log.error("Batch %d failed twice, skipping", batch_num)
                continue

        # Update DGraph
        updated = update_categories(classifications)
        total_classified += updated

        # Save progress
        for item in classifications:
            progress[item.get("name", "")] = item.get("categories", [])

        if batch_num % 10 == 0:
            with open(progress_file, "w") as f:
                json.dump(progress, f)
            log.info("  Progress saved. Classified %d so far.", total_classified)

        # Small delay to avoid overwhelming Claude CLI
        time.sleep(0.5)

    # Final progress save
    with open(progress_file, "w") as f:
        json.dump(progress, f)

    log.info("=== CLASSIFICATION COMPLETE ===")
    log.info("Total classified: %d", total_classified)

    # Print distribution
    log.info("Category distribution:")
    r = gql("{ queryCategory { name servers { name } } }")
    for cat in sorted(r["data"]["queryCategory"], key=lambda c: len(c.get("servers", [])), reverse=True):
        count = len(cat.get("servers", []))
        log.info("  %s: %d servers", cat["name"], count)


if __name__ == "__main__":
    main()
