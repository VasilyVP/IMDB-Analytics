"""ChromaDB collection inspection helper.

Use this script to quickly inspect a ChromaDB collection from the terminal.

Prerequisites:
- ChromaDB server is running and reachable.
- Collection exists in tenant "default_tenant" and database "default_database".

Examples:
- Print total records:
    uv run python scripts/chroma_info --count

- Print last 10 records (default):
    uv run python scripts/chroma_info --tail

- Print last 25 records from a custom collection/host/port:
    uv run python scripts/chroma_info --tail -n 25 --collection titles --host localhost --port 8001
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_HOST = os.getenv("CHROMA_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("CHROMA_PORT", "8001"))
DEFAULT_COLLECTION = os.getenv("CHROMA_COLLECTION", "titles")
TENANT = "default_tenant"
DATABASE = "default_database"

JsonDict = dict[str, Any]

try:
    from rich.console import Console as _RichConsole
    from rich.json import JSON as _RichJSON
except ImportError:
    _RichConsole = None
    _RichJSON = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect ChromaDB collection records")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--count", action="store_true", help="Print total number of records")
    group.add_argument("--tail", action="store_true", help="Print the last N records")

    parser.add_argument("-n", type=int, default=10, help="Number of records for --tail")
    return parser


def _request_json(url: str, method: str = "GET", body: JsonDict | None = None) -> Any:
    payload = None
    headers: dict[str, str] = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url=url, data=payload, headers=headers, method=method)
    with urlopen(request, timeout=15) as response:  # noqa: S310
        raw = response.read().decode("utf-8")
        if not raw:
            return None
        return json.loads(raw)


def _collection_info(base_url: str, collection_name: str) -> dict[str, Any]:
    url = (
        f"{base_url}/api/v2/tenants/{TENANT}/databases/{DATABASE}/collections"
        f"/{collection_name}"
    )
    response = _request_json(url)
    if not isinstance(response, dict):
        raise TypeError("Collection response is not an object")
    return cast(JsonDict, response)


def _coerce_int(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("Boolean cannot be used as an integer count")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError("Unsupported count response type")


def _coerce_object(value: Any) -> JsonDict:
    if not isinstance(value, dict):
        raise TypeError("Expected JSON object response")
    return cast(JsonDict, value)


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return cast(list[Any], value)
    return []


def _count(base_url: str, collection_id: str) -> int:
    url = (
        f"{base_url}/api/v2/tenants/{TENANT}/databases/{DATABASE}/collections"
        f"/{collection_id}/count"
    )
    response = _request_json(url)
    if isinstance(response, dict):
        payload = cast(JsonDict, response)
        return _coerce_int(payload.get("count"))
    return _coerce_int(response)


def _tail(base_url: str, collection_id: str, limit: int) -> list[dict[str, Any]]:
    total = _count(base_url, collection_id)
    if total <= 0:
        return []

    safe_limit = min(limit, total)
    offset = max(total - safe_limit, 0)
    url = (
        f"{base_url}/api/v2/tenants/{TENANT}/databases/{DATABASE}/collections"
        f"/{collection_id}/get"
    )
    raw_response = _request_json(
        url,
        method="POST",
        body={"limit": safe_limit, "offset": offset, "include": ["metadatas", "documents"]},
    )
    response = _coerce_object(raw_response)

    ids = _coerce_list(response.get("ids"))
    documents = _coerce_list(response.get("documents"))
    metadatas = _coerce_list(response.get("metadatas"))

    items: list[dict[str, Any]] = []
    for index, item_id in enumerate(ids):
        item_id_str = str(item_id)
        items.append(
            {
                "id": item_id_str,
                "document": documents[index] if index < len(documents) else None,
                "metadata": metadatas[index] if index < len(metadatas) else None,
            }
        )
    return items


def _print_json(data: Any) -> None:
    if _RichConsole is None or _RichJSON is None:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    console = _RichConsole()
    console.print(_RichJSON.from_data(data))


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.n <= 0:
        print("-n must be a positive integer", file=sys.stderr)
        return 2

    base_url = f"http://{args.host}:{args.port}"

    try:
        info = _collection_info(base_url, args.collection)
        collection_id = info["id"]

        if args.count:
            print(_count(base_url, collection_id))
            return 0

        _print_json(_tail(base_url, collection_id, args.n))
        return 0
    except HTTPError as exc:
        print(f"ChromaDB HTTP error: {exc.code} {exc.reason}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"ChromaDB connection error: {exc.reason}", file=sys.stderr)
        return 1
    except (KeyError, TypeError, ValueError) as exc:
        print(f"Unexpected ChromaDB response: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
