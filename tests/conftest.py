"""Shared test helpers. Tests run fully offline: no network, no AWS.

Async code is exercised with a tiny `run()` wrapper rather than a pytest-asyncio
plugin, so the only dev dependency is pytest itself.
"""
import asyncio
import sys
from pathlib import Path

import httpx

# Make the src/ layout importable without an editable install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def run(coro):
    """Run a coroutine to completion on a fresh event loop."""
    return asyncio.new_event_loop().run_until_complete(coro)


def mock_client(handler) -> httpx.AsyncClient:
    """An httpx.AsyncClient whose requests are served by `handler(request)`."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
