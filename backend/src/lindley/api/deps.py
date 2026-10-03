"""Shared request dependencies."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Request

from lindley.db.database import connect


def get_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """A database connection for one request."""
    conn = connect(request.app.state.settings.db_path, any_thread=True)
    try:
        yield conn
    finally:
        conn.close()


Conn = Annotated[sqlite3.Connection, Depends(get_conn)]
