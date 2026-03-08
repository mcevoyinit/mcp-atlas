#!/usr/bin/env python3
"""
MCP Tool Extractor — Extract tool schemas from MCP servers.

Connects to each MCP server via stdio, sends initialize + tools/list,
and stores the extracted tool schemas in DGraph.

Usage:
    python tool_extractor.py [--limit N] [--dry-run] [--verbose]

Requires: DGraph on localhost:18080, npx, uvx
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request

DGRAPH_URL = os.environ.get("DGRAPH_URL", "http://localhost:18080")
TIMEOUT_SECONDS = 30
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def gql(query: str, variables: dict | None = None) -> dict:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        f"{DGRAPH_URL}/graphql",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())


def get_candidates(limit: int) -> list[dict]:
    """Get top servers by quality score that have installable packages."""
    # Fetch more than needed — we filter for installable packages in Python
    # because DGraph hash search doesn't support "not empty" queries
    r = gql(
        """query($limit: Int!) {
            queryServer(
                order: { desc: qualityScore },
                first: $limit
            ) {
                name displayName qualityScore language
                npmPackage pypiPackage transport
                hasTools { name }
            }
        }""",
        {"limit": limit * 3},
    )
    servers = r.get("data", {}).get("queryServer", [])
    # Filter to installable (has npm or PyPI package) and no tools yet
    return [
        s for s in servers
        if (s.get("npmPackage") or s.get("pypiPackage"))
        and not s.get("hasTools")
    ]


def build_command(server: dict) -> list[str] | None:
    """Build the shell command to start an MCP server."""
    npm_pkg = server.get("npmPackage")
    pypi_pkg = server.get("pypiPackage")
    lang = (server.get("language") or "").lower()

    if npm_pkg and lang == "typescript":
        return ["npx", "-y", npm_pkg]
    elif pypi_pkg and lang == "python":
        return ["uvx", pypi_pkg]
    elif npm_pkg:
        # Default to npx for unknown language with npm package
        return ["npx", "-y", npm_pkg]
    elif pypi_pkg:
        return ["uvx", pypi_pkg]
    return None


def extract_tools(cmd: list[str], timeout: int = TIMEOUT_SECONDS) -> dict:
    """
    Start an MCP server, send initialize + tools/list, return results.

    Both the TypeScript and Python MCP SDKs exit cleanly when stdin closes,
    so subprocess.run() works — no need for Popen complexity.

    Returns:
        {"tools": [...], "error": None} on success
        {"tools": [], "error": "message"} on failure
    """
    init_msg = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "mcp-atlas-extractor", "version": "0.1"},
        },
    })
    initialized_msg = json.dumps({
        "jsonrpc": "2.0", "method": "notifications/initialized",
    })
    tools_msg = json.dumps({
        "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {},
    })
    stdin_data = f"{init_msg}\n{initialized_msg}\n{tools_msg}\n"

    try:
        proc = subprocess.run(
            cmd,
            input=stdin_data,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "NODE_NO_WARNINGS": "1"},
        )
    except subprocess.TimeoutExpired:
        return {"tools": [], "error": "timeout"}
    except FileNotFoundError:
        return {"tools": [], "error": f"command not found: {cmd[0]}"}
    except Exception as e:
        return {"tools": [], "error": str(e)}

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    # Find the tools/list response (id=2) in stdout
    for line in stdout.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            if msg.get("id") == 2:
                if "error" in msg:
                    return {"tools": [], "error": msg["error"].get("message", str(msg["error"]))}
                tools = msg.get("result", {}).get("tools", [])
                return {"tools": tools, "error": None}
        except json.JSONDecodeError:
            continue

    # No tools/list response found — diagnose the failure
    # Filter out package manager noise from stderr
    noise_patterns = [
        "WARN", "warn", "npm", "Downloading", "Downloaded",
        "Building", "Resolved", "Prepared", "Installed", "Audited",
        "packages in", "added ", "up to date", "deprecated",
    ]
    real_errors = []
    for errline in stderr.split("\n"):
        errline = errline.strip()
        if errline and not any(pat in errline for pat in noise_patterns):
            real_errors.append(errline)

    if real_errors:
        return {"tools": [], "error": real_errors[0][:200]}

    if not stdout.strip():
        return {"tools": [], "error": "no output (server may need config/API key)"}

    return {"tools": [], "error": f"tools/list response not found (stdout: {len(stdout)} bytes)"}


def save_tools_to_dgraph(server_name: str, tools: list[dict]) -> bool:
    """Save extracted tools to DGraph."""
    tool_inputs = []
    for t in tools:
        tool_input = {
            "name": t.get("name", ""),
            "title": t.get("name", ""),
            "description": (t.get("description") or "")[:500],
        }
        # Store the input schema as a JSON string in description if it has params
        schema = t.get("inputSchema", {})
        props = schema.get("properties", {})
        if props:
            param_names = list(props.keys())
            required = schema.get("required", [])
            param_summary = ", ".join(
                f"{p}{'*' if p in required else ''}" for p in param_names
            )
            if tool_input["description"]:
                tool_input["description"] += f" | Params: {param_summary}"
            else:
                tool_input["description"] = f"Params: {param_summary}"
        tool_inputs.append(tool_input)

    mutation = """
        mutation UpdateTools($name: String!, $tools: [ToolRef!]!) {
            updateServer(
                input: {
                    filter: { name: { eq: $name } },
                    set: { hasTools: $tools }
                }
            ) { numUids }
        }
    """
    try:
        r = gql(mutation, {"name": server_name, "tools": tool_inputs})
        num = r.get("data", {}).get("updateServer", {}).get("numUids", 0)
        return num > 0
    except Exception as e:
        log.error(f"DGraph update failed for {server_name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Extract MCP tool schemas")
    parser.add_argument("--limit", type=int, default=50, help="Max servers to process")
    parser.add_argument("--dry-run", action="store_true", help="Extract but don't save to DGraph")
    parser.add_argument("--verbose", action="store_true", help="Show full tool details")
    args = parser.parse_args()

    log.info(f"Getting top {args.limit} installable servers without tools...")
    candidates = get_candidates(args.limit * 2)  # Fetch extra since some will fail
    candidates = candidates[:args.limit]
    log.info(f"Found {len(candidates)} candidates")

    results = {"success": 0, "failed": 0, "skipped": 0, "details": []}

    for i, server in enumerate(candidates, 1):
        name = server["name"]
        pkg = server.get("npmPackage") or server.get("pypiPackage")
        score = server.get("qualityScore", 0)

        cmd = build_command(server)
        if not cmd:
            log.warning(f"[{i}/{len(candidates)}] SKIP {name}: no installable package")
            results["skipped"] += 1
            continue

        log.info(f"[{i}/{len(candidates)}] {name} (score={score:.3f}, cmd={' '.join(cmd)})")

        extraction = extract_tools(cmd)

        if extraction["error"]:
            log.warning(f"  FAIL: {extraction['error']}")
            results["failed"] += 1
            results["details"].append({
                "name": name, "package": pkg, "status": "failed",
                "error": extraction["error"],
            })
        else:
            tools = extraction["tools"]
            tool_names = [t["name"] for t in tools]
            log.info(f"  OK: {len(tools)} tools: {', '.join(tool_names[:5])}" +
                     ("..." if len(tool_names) > 5 else ""))

            if args.verbose:
                for t in tools:
                    desc = (t.get("description") or "")[:80]
                    log.info(f"    {t['name']}: {desc}")

            if not args.dry_run:
                saved = save_tools_to_dgraph(name, tools)
                if saved:
                    log.info(f"  Saved to DGraph")
                else:
                    log.warning(f"  DGraph save failed")

            results["success"] += 1
            results["details"].append({
                "name": name, "package": pkg, "status": "success",
                "toolCount": len(tools), "tools": tool_names,
            })

    # Summary
    log.info("=" * 50)
    log.info(f"COMPLETE: {results['success']} success, {results['failed']} failed, {results['skipped']} skipped")

    # Save results to file
    results_file = os.path.join(DATA_DIR, "extraction_results.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"Results saved to {results_file}")

    # Show error distribution
    errors = {}
    for d in results["details"]:
        if d["status"] == "failed":
            err = d.get("error", "unknown")
            # Normalize error messages
            if "timeout" in err.lower():
                key = "timeout"
            elif "not found" in err.lower() or "ENOENT" in err:
                key = "package not found"
            elif "api" in err.lower() or "key" in err.lower() or "token" in err.lower():
                key = "needs API key/config"
            elif "no output" in err.lower():
                key = "no output (needs config)"
            else:
                key = err[:60]
            errors[key] = errors.get(key, 0) + 1

    if errors:
        log.info("Error distribution:")
        for err, count in sorted(errors.items(), key=lambda x: -x[1]):
            log.info(f"  {err}: {count}")


if __name__ == "__main__":
    main()
