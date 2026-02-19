"""
Tavily Web Search agent.

Protocol:
- initialize: returns SERVICE_CARD
- compute: expects params with a "query" field (string),
  either as a dict or as a single-element list [query].

The agent calls the Tavily Search API and returns a structured JSON result.

Environment:
- TAVILY_API_KEY must be set to a valid Tavily API key.
"""

import json
import os
import sys
from typing import Any, Dict
from urllib import request, error as urlerror

from prxs_sdk.utils import call_llm_embeddings


SERVICE_CARD = {
    "name": "TavilySearch-v1",
    "description": "Performs web search via Tavily and returns relevant results.",
    "inputs": ["query"],
    "cost_per_op": 0.2,
    "version": "1.0.0",
    "tags": ["web-search", "research", "news", "tavily"],
}


def _attach_embedding_via_llm() -> None:
    """
    Optionally compute an embedding for this service using LLM Embeddings API.
    Best-effort: if API key or library are missing, agent still works.
    """
    if call_llm_embeddings is None:
        return

    try:
        text = "{} {} {}".format(
            SERVICE_CARD["name"],
            SERVICE_CARD.get("description", ""),
            " ".join(SERVICE_CARD.get("tags", [])),
        ).strip()
        vec = call_llm_embeddings(text)
        SERVICE_CARD["embedding"] = vec
    except Exception:
        return


_attach_embedding_via_llm()


def build_params(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        if raw:
            return {"query": raw[0]}
        return {}
    return {}


def call_tavily(query: str) -> Dict[str, Any]:
    api_key = os.environ.get("TAVILY_API_KEY", "")
    if not api_key:
        raise RuntimeError("TAVILY_API_KEY is not set")

    url = "https://api.tavily.com/search"
    payload = {
        "api_key": api_key,
        "query": query,
        "search_depth": "basic",
        "max_results": 5,
        "include_answer": True,
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = request.Request(url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
    except urlerror.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {"error": f"HTTP {e.code}", "body": body}
    except urlerror.URLError as e:
        raise RuntimeError(f"Connection error: {e.reason}") from e


def process_request(req: Dict[str, Any]) -> Dict[str, Any]:
    method = req.get("method")
    params = build_params(req.get("params"))
    resp: Dict[str, Any] = {"id": req.get("id"), "result": None, "error": None}

    if method == "initialize":
        resp["result"] = SERVICE_CARD
        return resp

    if method != "compute":
        resp["error"] = "Method not found"
        return resp

    try:
        query = params.get("query")
        if not query:
            raise ValueError("query is required")

        result = call_tavily(str(query))
        resp["result"] = result
    except Exception as exc:  # pylint: disable=broad-except
        resp["error"] = str(exc)

    return resp


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            req = json.loads(line)
            response = process_request(req)
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
        except json.JSONDecodeError:
            continue


if __name__ == "__main__":
    main()
