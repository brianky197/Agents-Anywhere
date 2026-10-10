"""In-process stand-ins for the Server side of Connector ingest tests."""

from __future__ import annotations

import httpx

MANIFEST_PATH = "/connector/timeline/manifest"


def old_server(handler) -> httpx.MockTransport:
    """A Server from before the timeline manifest: the route does not exist.

    Complete snapshots are then sent whole, so ``handler`` sees exactly the
    requests it saw before issue #278 was reconciled.
    """

    def route(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(MANIFEST_PATH):
            return httpx.Response(404, json={"detail": "Not Found"})
        return handler(request)

    return httpx.MockTransport(route)
