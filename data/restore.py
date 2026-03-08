#!/usr/bin/env python3
"""
Restore MCP catalog data into a fresh DGraph instance.

Usage:
    python3 restore.py [DGRAPH_URL] [GROOT_PASSWORD]

Defaults:
    DGRAPH_URL = http://localhost:8080
    GROOT_PASSWORD = (none - no ACL)
"""

import json
import sys
import time
import urllib.request

DATA_DIR = __import__("os").path.dirname(__import__("os").path.abspath(__file__))
DGRAPH_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
GROOT_PASSWORD = sys.argv[2] if len(sys.argv) > 2 else None

print(f"DGraph URL: {DGRAPH_URL}")
print(f"ACL: {'enabled' if GROOT_PASSWORD else 'disabled'}")


def make_request(url, data, headers=None):
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=hdrs)
    return json.loads(urllib.request.urlopen(req, timeout=60).read())


# ── Step 1: Authenticate (if ACL enabled) ──
jwt = None
if GROOT_PASSWORD:
    print("Authenticating...")
    r = make_request(f"{DGRAPH_URL}/admin", {
        "query": f'mutation {{ login(userId: "groot", password: "{GROOT_PASSWORD}") {{ response {{ accessJWT }} }} }}'
    })
    jwt = r["data"]["login"]["response"]["accessJWT"]
    print("  Authenticated")

auth_headers = {"X-Dgraph-AccessToken": jwt} if jwt else {}


def gql(query, variables=None):
    return make_request(f"{DGRAPH_URL}/graphql", {"query": query, "variables": variables or {}}, auth_headers)


def admin(query):
    return make_request(f"{DGRAPH_URL}/admin", {"query": query}, auth_headers)


# ── Step 2: Deploy schema ──
print("Deploying schema...")
with open(f"{DATA_DIR}/schema.graphql") as f:
    schema = f.read()

r = admin('mutation U($sch: String!) { updateGQLSchema(input: { set: { schema: $sch } }) { gqlSchema { schema } } }')
# If variable substitution doesn't work, try the direct approach
if r.get("errors"):
    import urllib.parse
    payload = {"query": f'mutation {{ updateGQLSchema(input: {{ set: {{ schema: """{schema}""" }} }}) {{ gqlSchema {{ schema }} }} }}'}
    r = make_request(f"{DGRAPH_URL}/admin", payload, auth_headers)

print("  Schema deployed")
time.sleep(2)  # Let schema propagate

# Re-auth after schema change (token may be invalidated)
if GROOT_PASSWORD:
    r = make_request(f"{DGRAPH_URL}/admin", {
        "query": f'mutation {{ login(userId: "groot", password: "{GROOT_PASSWORD}") {{ response {{ accessJWT }} }} }}'
    })
    jwt = r["data"]["login"]["response"]["accessJWT"]
    auth_headers = {"X-Dgraph-AccessToken": jwt}


# ── Step 3: Load categories ──
print("Loading categories...")
with open(f"{DATA_DIR}/categories.json") as f:
    categories = json.load(f)

cat_inputs = []
for c in categories:
    entry = {"name": c["name"], "slug": c["slug"]}
    if c.get("description"):
        entry["description"] = c["description"]
    if c.get("displayOrder") is not None:
        entry["displayOrder"] = c["displayOrder"]
    cat_inputs.append(entry)

r = gql("mutation A($input: [AddCategoryInput!]!) { addCategory(input: $input, upsert: true) { numUids } }",
        {"input": cat_inputs})
print(f"  Loaded {len(cat_inputs)} categories")


# ── Step 4: Load stacks ──
print("Loading stacks...")
with open(f"{DATA_DIR}/stacks.json") as f:
    stacks = json.load(f)

stack_inputs = [{"name": s["name"], "slug": s["slug"]} for s in stacks]
r = gql("mutation A($input: [AddStackInput!]!) { addStack(input: $input, upsert: true) { numUids } }",
        {"input": stack_inputs})
print(f"  Loaded {len(stack_inputs)} stacks")


# ── Step 5: Load servers (batched) ──
print("Loading servers...")
with open(f"{DATA_DIR}/servers.json") as f:
    servers = json.load(f)

BATCH = 50
loaded = 0
errors = 0

for i in range(0, len(servers), BATCH):
    batch = servers[i:i + BATCH]
    inputs = []
    for s in batch:
        entry = {}
        # Copy scalar fields
        for key in ["name", "displayName", "description", "githubUrl", "npmPackage",
                     "pypiPackage", "homepage", "language", "license", "version",
                     "bestFor", "pricing"]:
            if s.get(key):
                entry[key] = s[key]
        # Copy list fields
        if s.get("transport"):
            entry["transport"] = s["transport"]
        # Copy numeric fields
        for key in ["stars", "downloadsWeekly", "downloadsMonthly", "useCount"]:
            if s.get(key) is not None:
                entry[key] = s[key]
        # Copy float fields
        if s.get("qualityScore") is not None:
            entry["qualityScore"] = s["qualityScore"]
        # Copy datetime fields
        for key in ["lastCommit", "createdAt"]:
            if s.get(key):
                entry[key] = s[key]
        # Copy listings
        if s.get("listings"):
            entry["listings"] = []
            for lst in s["listings"]:
                le = {}
                for k in ["registry", "registryId", "registryUrl", "lastCrawled",
                           "securityGrade", "qualityGrade"]:
                    if lst.get(k):
                        le[k] = lst[k]
                for k in ["verified", "isDeployed"]:
                    if lst.get(k) is not None:
                        le[k] = lst[k]
                if lst.get("trafficWeekly") is not None:
                    le["trafficWeekly"] = lst["trafficWeekly"]
                entry["listings"].append(le)
        inputs.append(entry)

    r = gql("mutation A($input: [AddServerInput!]!) { addServer(input: $input, upsert: true) { numUids } }",
            {"input": inputs})
    if r.get("errors"):
        errors += len(batch)
        print(f"  Error at batch {i}: {r['errors'][0]['message'][:100]}")
    else:
        loaded += r.get("data", {}).get("addServer", {}).get("numUids", 0)

    if (i + BATCH) % 500 == 0 or i + BATCH >= len(servers):
        print(f"  Progress: {min(i + BATCH, len(servers))}/{len(servers)} servers...")

print(f"  Loaded {loaded} servers ({errors} errors)")
print("\nRestore complete!")
