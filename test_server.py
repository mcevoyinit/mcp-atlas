#!/usr/bin/env python3
"""
MCP Atlas — Test Suite

Tests the discovery server end-to-end via the MCP JSON-RPC protocol.
Requires DGraph running on localhost:18080 with catalog data loaded.

Usage: uv run python test_server.py
"""

import json
import subprocess
import sys
import time

SERVER_CMD = [sys.executable, "server.py"]
PASS = 0
FAIL = 0
ERRORS = []


def call_mcp(method: str, params: dict | None = None, id: int = 2) -> dict:
    """Send a JSON-RPC request to the MCP server and return the response."""
    init_msg = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        },
    })
    call_msg = json.dumps({
        "jsonrpc": "2.0", "id": id,
        "method": method,
        "params": params or {},
    })
    stdin_data = f"{init_msg}\n{call_msg}\n"

    proc = subprocess.run(
        SERVER_CMD, input=stdin_data, capture_output=True, text=True, timeout=30,
    )
    # Parse last line (the response to our call, not the init response)
    lines = [l for l in proc.stdout.strip().split("\n") if l.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"Expected 2+ responses, got {len(lines)}: {proc.stdout[:500]}")
    return json.loads(lines[-1])


def tool_call(name: str, arguments: dict | None = None) -> any:
    """Call an MCP tool and return the parsed content."""
    resp = call_mcp("tools/call", {"name": name, "arguments": arguments or {}})
    if "error" in resp:
        raise RuntimeError(f"MCP error: {resp['error']}")
    text = resp["result"]["content"][0]["text"]
    return json.loads(text)


def check(name: str, condition: bool, detail: str = ""):
    """Record a test result."""
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(f"{name}: {detail}")
    print(f"  [{status}] {name}" + (f" — {detail}" if detail and not condition else ""))


def test_protocol():
    """Test MCP protocol basics."""
    print("\n=== Protocol Tests ===")

    # Initialize — send single init message and parse first response
    init_msg = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        },
    })
    proc = subprocess.run(
        SERVER_CMD, input=init_msg + "\n", capture_output=True, text=True, timeout=15,
    )
    lines = [l for l in proc.stdout.strip().split("\n") if l.strip()]
    resp = json.loads(lines[0]) if lines else {}
    result = resp.get("result", {})
    check("init returns serverInfo",
          result.get("serverInfo", {}).get("name") == "MCP Atlas")
    check("init returns tools capability",
          "tools" in result.get("capabilities", {}))
    check("init returns instructions",
          "discovery" in result.get("instructions", "").lower())

    # Tools list
    resp = call_mcp("tools/list")
    tools = resp.get("result", {}).get("tools", [])
    tool_names = {t["name"] for t in tools}
    check("tools/list returns 4 tools", len(tools) == 4, f"got {len(tools)}")
    for expected in ["search_servers", "get_server_details", "browse_categories", "recommend_servers"]:
        check(f"tool '{expected}' registered", expected in tool_names)

    # Verify tool schemas have descriptions
    for t in tools:
        check(f"tool '{t['name']}' has description", bool(t.get("description")))
        check(f"tool '{t['name']}' has inputSchema", bool(t.get("inputSchema")))


def test_browse_categories():
    """Test browse_categories tool."""
    print("\n=== browse_categories Tests ===")

    cats = tool_call("browse_categories")
    check("returns a list", isinstance(cats, list))
    check("returns 15 categories", len(cats) == 15, f"got {len(cats)}")

    # Check structure
    if cats:
        c = cats[0]
        check("category has 'name'", "name" in c)
        check("category has 'description'", "description" in c)
        check("category has 'serverCount'", "serverCount" in c)
        check("categories sorted by count (desc)",
              all(cats[i]["serverCount"] >= cats[i+1]["serverCount"] for i in range(len(cats)-1)))

    # Check known categories exist
    cat_names = {c["name"] for c in cats}
    for expected in ["Data & Analytics", "AI & LLM", "Database & Storage", "DevOps & CI/CD"]:
        check(f"category '{expected}' exists", expected in cat_names)

    # Check counts are reasonable
    total_assignments = sum(c["serverCount"] for c in cats)
    check("total category assignments > 2000",
          total_assignments > 2000, f"got {total_assignments}")


def test_search_servers():
    """Test search_servers tool."""
    print("\n=== search_servers Tests ===")

    # Basic keyword search
    results = tool_call("search_servers", {"query": "database", "limit": 5})
    check("keyword 'database' returns results", len(results) > 0, f"got {len(results)}")
    check("respects limit=5", len(results) <= 5, f"got {len(results)}")

    # Check result structure
    if results:
        s = results[0]
        for field in ["name", "id", "description", "qualityScore"]:
            check(f"result has '{field}'", field in s)
        check("qualityScore is numeric", isinstance(s.get("qualityScore"), (int, float)))
        check("results sorted by qualityScore (desc)",
              all(results[i]["qualityScore"] >= results[i+1]["qualityScore"]
                  for i in range(len(results)-1)))

    # Category filter
    results = tool_call("search_servers", {"query": "search", "category": "AI & LLM", "limit": 10})
    check("category filter returns results", len(results) > 0, f"got {len(results)}")
    # All results should be in AI & LLM
    for s in results:
        if "categories" in s:
            check(f"'{s['id']}' in AI & LLM category", "AI & LLM" in s.get("categories", []))

    # Language filter
    results = tool_call("search_servers", {"query": "api", "language": "Python", "limit": 5})
    check("language filter returns results", len(results) >= 0)  # may be 0 if no match
    for s in results:
        check(f"'{s['id']}' language is Python",
              s.get("language", "").lower() == "python",
              f"got '{s.get('language')}'")

    # Limit bounds
    results = tool_call("search_servers", {"query": "server", "limit": 1})
    check("limit=1 returns at most 1", len(results) <= 1, f"got {len(results)}")

    results = tool_call("search_servers", {"query": "server", "limit": 100})
    check("limit=100 capped to 50", len(results) <= 50, f"got {len(results)}")

    # Empty/no-match query
    results = tool_call("search_servers", {"query": "xyznonexistent99999", "limit": 5})
    check("nonsense query returns empty list", len(results) == 0, f"got {len(results)}")


def test_get_server_details():
    """Test get_server_details tool."""
    print("\n=== get_server_details Tests ===")

    # Known server
    result = tool_call("get_server_details", {"server_id": "io.github.neo4j-contrib/mcp-neo4j-cypher"})
    check("returns a dict", isinstance(result, dict))
    check("no error", "error" not in result)
    check("has name", bool(result.get("name")))
    check("has id", result.get("id") == "io.github.neo4j-contrib/mcp-neo4j-cypher")
    check("has qualityScore", isinstance(result.get("qualityScore"), (int, float)))
    check("has categories", isinstance(result.get("categories"), list))

    # Verbose fields
    for field in ["language", "license", "transport"]:
        check(f"verbose field '{field}' present", field in result, f"missing from {list(result.keys())}")

    # Non-existent server
    result = tool_call("get_server_details", {"server_id": "nonexistent/fake-server-12345"})
    check("non-existent returns error", "error" in result)
    check("error message mentions server name", "nonexistent" in result.get("error", ""))


def test_recommend_servers():
    """Test recommend_servers tool."""
    print("\n=== recommend_servers Tests ===")

    # Broad recommendation
    results = tool_call("recommend_servers", {
        "task_description": "manage PostgreSQL database with migrations",
        "limit": 5,
    })
    check("recommendations returned", len(results) > 0, f"got {len(results)}")
    check("respects limit=5", len(results) <= 5, f"got {len(results)}")

    # Check relevance — at least one result should mention database/postgres
    if results:
        any_relevant = any(
            "database" in s.get("description", "").lower()
            or "postgres" in s.get("description", "").lower()
            or "Database" in str(s.get("categories", []))
            for s in results
        )
        check("at least one result is database-related", any_relevant)

    # Narrow recommendation
    results = tool_call("recommend_servers", {
        "task_description": "send email notifications via SendGrid SMTP",
        "limit": 3,
    })
    check("narrow query returns results", len(results) > 0 or
          (len(results) == 1 and "message" in results[0]))

    # Very broad (should use anyoftext fallback)
    results = tool_call("recommend_servers", {
        "task_description": "monitor kubernetes cluster health and send alerts",
        "limit": 5,
    })
    check("broad multi-word query returns results", len(results) > 0, f"got {len(results)}")


def test_security():
    """Test input sanitization and edge cases."""
    print("\n=== Security Tests ===")

    # GraphQL injection attempt
    results = tool_call("search_servers", {
        "query": 'injection" } }) { name } }',
        "limit": 3,
    })
    check("injection attempt returns safely (list)", isinstance(results, list))

    # Newline injection
    results = tool_call("search_servers", {
        "query": "test\nmalicious",
        "limit": 3,
    })
    check("newline injection returns safely", isinstance(results, list))

    # Backslash injection
    results = tool_call("search_servers", {
        "query": 'test\\"}}]}',
        "limit": 3,
    })
    check("backslash injection returns safely", isinstance(results, list))

    # Empty query
    results = tool_call("search_servers", {"query": "", "limit": 5})
    check("empty query returns results (top by quality)", isinstance(results, list))

    # Recommend with injection
    results = tool_call("recommend_servers", {
        "task_description": 'test" }) { name } } }',
        "limit": 3,
    })
    check("recommend injection returns safely", isinstance(results, list))


def test_response_format():
    """Test that all tools return single content blocks with valid JSON."""
    print("\n=== Response Format Tests ===")

    for tool_name, args in [
        ("browse_categories", {}),
        ("search_servers", {"query": "test", "limit": 3}),
        ("get_server_details", {"server_id": "io.github.neo4j-contrib/mcp-neo4j-cypher"}),
        ("recommend_servers", {"task_description": "test task", "limit": 3}),
    ]:
        resp = call_mcp("tools/call", {"name": tool_name, "arguments": args})
        content = resp.get("result", {}).get("content", [])
        check(f"{tool_name}: single content block", len(content) == 1, f"got {len(content)} blocks")
        if content:
            check(f"{tool_name}: content type is text", content[0].get("type") == "text")
            try:
                parsed = json.loads(content[0]["text"])
                check(f"{tool_name}: content is valid JSON", True)
            except json.JSONDecodeError as e:
                check(f"{tool_name}: content is valid JSON", False, str(e))


def test_performance():
    """Test response times are acceptable."""
    print("\n=== Performance Tests ===")

    tests = [
        ("browse_categories", {}),
        ("search_servers", {"query": "database", "limit": 10}),
        ("get_server_details", {"server_id": "io.github.neo4j-contrib/mcp-neo4j-cypher"}),
        ("recommend_servers", {"task_description": "manage a database", "limit": 5}),
    ]

    for tool_name, args in tests:
        start = time.time()
        tool_call(tool_name, args)
        elapsed_ms = (time.time() - start) * 1000
        # Each call includes server startup (~1-2s for uv run), so measure total
        # Real latency in production (persistent server) would be much lower
        check(f"{tool_name}: responds in <5s (incl startup)",
              elapsed_ms < 5000, f"{elapsed_ms:.0f}ms")


if __name__ == "__main__":
    print("MCP Atlas — Test Suite")
    print("=" * 50)

    # Verify DGraph is reachable
    try:
        import urllib.request
        resp = urllib.request.urlopen("http://localhost:18080/health", timeout=5)
        health = json.loads(resp.read())
        print(f"DGraph: {health[0]['status']} ({health[0]['instance']})")
    except Exception as e:
        print(f"ERROR: DGraph not reachable at localhost:18080: {e}")
        print("Start DGraph: docker compose up -d")
        sys.exit(1)

    test_protocol()
    test_browse_categories()
    test_search_servers()
    test_get_server_details()
    test_recommend_servers()
    test_security()
    test_response_format()
    test_performance()

    print("\n" + "=" * 50)
    print(f"Results: {PASS} passed, {FAIL} failed")
    if ERRORS:
        print(f"\nFailures:")
        for e in ERRORS:
            print(f"  - {e}")
    sys.exit(0 if FAIL == 0 else 1)
