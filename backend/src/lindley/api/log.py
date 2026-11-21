"""The browser's own errors, in Lindley's log with the rest.

A screen that crashes, or a promise nobody caught, would otherwise be seen only in the browser's
console, which nobody has open. The page sends a short report here; it's written to the log as
`lindley.frontend`, beside the request that came before it. Nothing leaves this computer, and a
runaway page can't fill the log: only so many reports are kept a minute, each only so long, and
anything that looks like a key is hidden (lindley.logs.redact).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Literal

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from lindley.logs import redact

router = APIRouter(prefix="/log", tags=["log"])
frontend_log = logging.getLogger("lindley.frontend")


class Limit:
    """At most `most` reports in `seconds`."""

    def __init__(self, most: int = 30, seconds: float = 60.0) -> None:
        self.most, self.seconds = most, seconds
        self._seen: deque[float] = deque()
        self._lock = threading.Lock()
        self._told = False

    def allow(self) -> bool:
        now = time.monotonic()
        with self._lock:
            while self._seen and now - self._seen[0] >= self.seconds:
                self._seen.popleft()
            if len(self._seen) < self.most:
                self._seen.append(now)
                self._told = False
                return True
            if not self._told:  # said once for each run of them, not for each
                self._told = True
                frontend_log.warning(
                    "The page is reporting errors faster than %d a minute: the rest are dropped",
                    self.most,
                )
            return False


limit = Limit()


class Report(BaseModel):
    level: Literal["error", "warning"] = "error"
    message: str = Field(min_length=1, max_length=500)
    stack: str | None = Field(default=None, max_length=4000)
    where: str | None = Field(default=None, max_length=200)  # the screen: its address, no query


def _one_line(text: str) -> str:
    """No line breaks to forge a log line with."""
    return " ".join(text.split())


@router.post("", status_code=204)
def report(body: Report) -> Response:
    if not limit.allow():
        raise HTTPException(429, "Too many reports")
    stack = ""
    if body.stack:
        lines = [redact(line.rstrip()) for line in body.stack.splitlines() if line.strip()]
        stack = "\n    " + "\n    ".join(lines[:30])
    frontend_log.log(
        logging.ERROR if body.level == "error" else logging.WARNING,
        "%s (on %s)%s",
        redact(_one_line(body.message)),
        _one_line(body.where or "an unknown screen"),
        stack,
    )
    return Response(status_code=204)
