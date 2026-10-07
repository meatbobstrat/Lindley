"""Lindley as an app: started from the menu, it runs in the background and opens in the browser.

Started again while it's running, it opens the page and leaves the one running be. Where the
system has a tray (Windows' notification area, the Mac's menu bar, Ubuntu's AppIndicators), an
icon there opens Lindley and quits it. Quit Lindley in the app quits it too (POST /api/quit).
"""

from __future__ import annotations

import logging
import threading
import webbrowser
from collections.abc import Callable
from importlib.resources import files

import httpx2
import uvicorn

log = logging.getLogger(__name__)

START_S = 60  # the longest the browser waits for Lindley to start


def address(host: str, port: int) -> str:
    """Lindley's address, for the browser."""
    shown = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    return f"http://{shown}:{port}/"


def already_running(url: str) -> bool:
    """Whether Lindley already answers there: started from the menu twice, it's one Lindley."""
    try:
        with httpx2.Client(timeout=2) as client:
            r = client.get(url + "api/health")
        return r.status_code == 200 and r.json().get("status") == "ok"
    except (httpx2.HTTPError, ValueError, AttributeError):
        return False


def run(server: uvicorn.Server, url: str, browser: bool = True, tray: bool = True) -> None:
    """Serve until Lindley is quit, opening it in the browser once it has started."""
    if browser:
        threading.Thread(target=_open_when_started, args=(server, url), daemon=True).start()
    icon = tray_icon(url, quit=lambda: setattr(server, "should_exit", True)) if tray else None
    if icon is None:
        server.run()
        return
    serving = threading.Thread(target=_serve, args=(server, icon), name="lindley-server")
    serving.start()
    try:
        icon.run()  # on this thread, as the Mac needs: until Quit, or Lindley stopping
    except Exception:  # no tray after all: Lindley runs on, quit from the app
        log.warning("Couldn't show Lindley's icon in the tray", exc_info=True)
    serving.join()


def _serve(server: uvicorn.Server, icon) -> None:
    try:
        server.run()
    finally:
        icon.stop()


def _open_when_started(server: uvicorn.Server, url: str) -> None:
    done = threading.Event()
    for _ in range(START_S * 10):
        if server.started:
            webbrowser.open(url)
            return
        if server.should_exit:
            return
        done.wait(0.1)


def tray_icon(url: str, quit: Callable[[], None]):
    """Lindley's icon in the tray, with Open and Quit; None where there's no tray (pystray isn't
    installed, or the system has nowhere to show one)."""
    try:
        import pystray
        from PIL import Image

        image = Image.open(files("lindley") / "icon.png")
        return pystray.Icon(
            "Lindley",
            image,
            "Lindley",
            menu=pystray.Menu(
                pystray.MenuItem("Open Lindley", lambda: webbrowser.open(url), default=True),
                pystray.MenuItem("Quit Lindley", lambda: quit()),
            ),
        )
    except Exception:  # pystray raises all sorts where it has no backend
        log.info("No tray here: quit Lindley from the app")
        return None
