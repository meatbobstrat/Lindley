"""What the connectors share: pages as images, and a failed call in words for a person.

The connectors call each AI through its company's own library (its SDK), which sends the
requests, reads the answers and tries again when the AI is busy. This module only turns what
went wrong into a ProviderError a person can read.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator
from contextlib import contextmanager

from lindley.providers.base import ProviderError

BUSY = (429, 503, 529)  # too many requests, unavailable, overloaded
BUSY_ERRORS = ("overloaded_error", "rate_limit_error")  # Anthropic, in a stream


def detail(body: object, fallback: object = "") -> str:
    """The message in an error's body ({"error": {"message": ...}} or {"message": ...})."""
    err = body.get("error", body) if isinstance(body, dict) else None
    if isinstance(err, dict) and err.get("message"):
        return str(err["message"])[:200]
    return str(fallback)[:200]


def failure(who: str, status: int | None, why: str) -> ProviderError:
    if status in BUSY:
        return ProviderError(f"{who} is busy ({status}), even after trying again: {why}")
    if status in (401, 403):
        return ProviderError(f"{who} refused the key ({status}): {why}")
    if status == 404:
        return ProviderError(f"{who} has no such model or address (404): {why}")
    return ProviderError(f"{who} answered {status}: {why}")


@contextmanager
def sdk_errors(sdk, who: str, where: str | None = None) -> Iterator[None]:
    """Errors from the Anthropic or OpenAI library (they're built the same way), in words."""
    try:
        yield
    except sdk.APITimeoutError as e:
        raise ProviderError(f"{who} took too long to answer") from e
    except sdk.APIConnectionError as e:
        at = f" at {where}" if where else ""
        raise ProviderError(f"Couldn't reach {who}{at}: {e}") from e
    except sdk.APIStatusError as e:
        status = e.status_code
        err = e.body.get("error") if isinstance(e.body, dict) else None
        if isinstance(err, dict) and err.get("type") in BUSY_ERRORS:  # sent mid-stream
            status = 529
        raise failure(who, status, detail(e.body, e.message)) from e


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
