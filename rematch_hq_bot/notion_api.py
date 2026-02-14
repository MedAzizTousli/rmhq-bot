from __future__ import annotations

from typing import Any

import httpx


NOTION_VERSION = "2022-06-28"


class NotionClient:
    def __init__(self, token: str):
        self._token = token

    async def retrieve_database(self, database_id: str) -> dict[str, Any]:
        url = f"https://api.notion.com/v1/databases/{database_id}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Notion-Version": NOTION_VERSION,
        }

        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            return r.json()

    async def query_database(self, database_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

        results: list[dict[str, Any]] = []
        start_cursor: str | None = None

        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                body = dict(payload)
                if start_cursor:
                    body["start_cursor"] = start_cursor

                r = await client.post(url, headers=headers, json=body)
                r.raise_for_status()
                data = r.json()

                results.extend(data.get("results", []))
                if not data.get("has_more"):
                    break
                start_cursor = data.get("next_cursor")
                if not start_cursor:
                    break

        return results

