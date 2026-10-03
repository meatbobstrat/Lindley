"""HTTP helpers the connectors share: one client, errors in words for a person, SSE lines."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from lindley.providers.base import ProviderBusy, ProviderError

BUSY = (429, 503, 529)  # too many requests, unavailable, overloaded


def make_client(config, client: httpx.Client | None) -> httpx.Client:
    return client or httpx.Client(timeout=config.timeout_s if config else 120)


def _retry_after(headers: httpx.Headers) -> float | None:
    v = headers.get("retry-after")
    if not v:
        return None
    try:
        return max(0.0, float(v))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(v) - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError):
        return None


def _detail(resp: httpx.Response) -> str:
    try:
        err = resp.json().get("error")
    except (json.JSONDecodeError, AttributeError):
        return resp.text[:200]
    if isinstance(err, dict):
        return str(err.get("message") or err)[:200]
    return str(err or resp.text)[:200]


def check_status(resp: httpx.Response, who: str) -> None:
    """Raise ProviderBusy or ProviderError, in words for a person, unless the call worked."""
    if resp.is_success:
        return
    resp.read()
    status, detail = resp.status_code, _detail(resp)
    if status in BUSY:
        raise ProviderBusy(f"{who} is busy ({status}): {detail}", _retry_after(resp.headers))
    if status in (401, 403):
        raise ProviderError(f"{who} refused the key ({status}): {detail}")
    if status == 404:
        raise ProviderError(f"{who} has no such model or address (404): {detail}")
    raise ProviderError(f"{who} answered {status}: {detail}")


@contextmanager
def reaching(who: str, url: str) -> Iterator[None]:
    """Network failures, in words for a person."""
    try:
        yield
    except httpx.TimeoutException as e:
        raise ProviderError(f"{who} took too long to answer") from e
    except httpx.HTTPError as e:
        raise ProviderError(f"Couldn't reach {who} at {url}: {e}") from e


def send(http: httpx.Client, method: str, url: str, who: str, **kw) -> dict:
    with reaching(who, url):
        resp = http.request(method, url, **kw)
    check_status(resp, who)
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        raise ProviderError(f"{who} sent back something that isn't JSON") from e


def stream_data(http: httpx.Client, url: str, who: str, **kw) -> Iterator[str]:
    """The `data:` lines of a server-sent event stream."""
    with reaching(who, url), http.stream("POST", url, **kw) as resp:
        check_status(resp, who)
        for line in resp.iter_lines():
            if line.startswith("data:"):
                yield line[5:].strip()


def image_type(data: bytes) -> str:
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    raise ProviderError("Pages are sent as JPEG, PNG or WebP; this image is none of those")


def b64(data: bytes) -> str:
    return base64.standard_b64encode(data).decode("ascii")
