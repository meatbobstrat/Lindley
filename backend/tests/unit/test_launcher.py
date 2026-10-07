"""Lindley as an app: started once, opened in the browser, quit from the app or the tray."""

import json
import socket
import subprocess
import sys
import time

import httpx2

from lindley import launcher


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_the_browser_goes_to_this_computer():
    assert launcher.address("127.0.0.1", 8765) == "http://127.0.0.1:8765/"
    assert launcher.address("0.0.0.0", 9000) == "http://127.0.0.1:9000/"


def test_another_program_on_the_port_isnt_lindley(monkeypatch):
    assert not launcher.already_running(f"http://127.0.0.1:{free_port()}/")

    real = httpx2.Client

    def answers(body: bytes):
        def client(**kw):
            kw["transport"] = httpx2.MockTransport(lambda r: httpx2.Response(200, content=body))
            return real(**kw)

        monkeypatch.setattr(launcher.httpx2, "Client", client)

    answers(b"<html>something else</html>")
    assert not launcher.already_running("http://127.0.0.1:8765/")
    answers(json.dumps({"status": "ok", "version": "0.1.0"}).encode())
    assert launcher.already_running("http://127.0.0.1:8765/")


def test_no_tray_without_pystray(monkeypatch):
    monkeypatch.setitem(sys.modules, "pystray", None)  # not installed
    assert launcher.tray_icon("http://127.0.0.1:8765/", quit=lambda: None) is None


def test_quit_isnt_offered_where_lindley_cant_stop_itself(client):
    assert client.get("/api/health").json()["can_quit"] is False  # no Quit in the app
    r = client.post("/api/quit")
    assert r.status_code == 409 and "can't be stopped from the app" in r.json()["detail"]
    asked = []
    client.app.state.quit = lambda: asked.append(True)
    assert client.get("/api/health").json()["can_quit"] is True
    assert client.post("/api/quit").json() == {"stopping": True} and asked == [True]


def test_started_then_quit_from_the_app(tmp_path):
    """The whole of it, as the menu starts it (less the browser and the tray): it serves, keeps
    a log beside the database, and stops when Quit Lindley is chosen."""
    data = tmp_path / "data"
    settings = {
        "watch_folders": [str(tmp_path / "inbox")],
        "processing_dir": str(data / "processing"),
        "quarantine_dir": str(data / "quarantine"),
        "library_dir": str(tmp_path / "library"),
        "db_path": str(data / "lindley.db"),
    }
    (tmp_path / "settings.json").write_text(json.dumps(settings), encoding="utf-8")
    port = free_port()
    url = f"http://127.0.0.1:{port}/"
    command = [sys.executable, "-m", "lindley", "--no-browser", "--no-tray", "--port", str(port)]
    proc = subprocess.Popen(
        [*command, "--settings", str(tmp_path / "settings.json")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 30
        while not launcher.already_running(url):
            assert proc.poll() is None and time.monotonic() < deadline, "Lindley didn't start"
            time.sleep(0.2)
        # Started again: it opens the one running, and is gone
        again = subprocess.run(
            command + ["--settings", str(tmp_path / "settings.json")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert again.returncode == 0 and "already running" in again.stdout
        with httpx2.Client() as c:
            assert c.post(url + "api/quit").json() == {"stopping": True}
        assert proc.wait(30) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    log = (data / "logs" / "lindley.log").read_text(encoding="utf-8")
    assert "Application startup complete" in log and "POST /api/quit" in log
