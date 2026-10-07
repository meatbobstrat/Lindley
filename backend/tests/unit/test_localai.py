"""Lindley's own AI: downloading its files, and running its server.

Downloads come from a made-up server (httpx2.MockTransport), and the "llama-server" is a small
Python script that answers /health, so nothing touches the network or needs llama.cpp.
"""

import hashlib
import io
import sys
import textwrap
import threading
import zipfile
from pathlib import Path

import httpx2
import pytest

from lindley import activity
from lindley.api import local_ai as local_ai_api
from lindley.config import LocalAiSettings
from lindley.localai import catalog, download, server
from lindley.localai.catalog import Engine, File, Model
from lindley.localai.download import Cancelled, DownloadError, Downloads, fetch
from lindley.providers.base import ProviderError


def file_of(name: str, data: bytes) -> File:
    return File(name, f"https://example.test/{name}", len(data), hashlib.sha256(data).hexdigest())


class Files:
    """Serves these files, with Range requests, and keeps the requests."""

    def __init__(self, files: dict[str, bytes], ranges: bool = True) -> None:
        self.files = files
        self.ranges = ranges
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        data = self.files[request.url.path.lstrip("/")]
        if self.ranges and (r := request.headers.get("range")):
            start = int(r.removeprefix("bytes=").rstrip("-"))
            return httpx2.Response(206, content=data[start:])
        return httpx2.Response(200, content=data)

    def client(self) -> httpx2.Client:
        return httpx2.Client(transport=httpx2.MockTransport(self))


DATA = bytes(range(256)) * 9000  # a little over 2 chunks


def test_fetch_downloads_and_checks(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    seen = []
    fetch(Files({"m.gguf": DATA}).client(), f, tmp_path / "m.gguf", seen.append)
    assert (tmp_path / "m.gguf").read_bytes() == DATA
    assert not (tmp_path / "m.gguf.part").exists()
    assert seen[0] == 0 and seen[-1] == len(DATA)


def test_fetch_goes_on_from_a_part(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    (tmp_path / "m.gguf.part").write_bytes(DATA[:1000])
    files = Files({"m.gguf": DATA})
    fetch(files.client(), f, tmp_path / "m.gguf")
    assert files.requests[0].headers["range"] == "bytes=1000-"
    assert (tmp_path / "m.gguf").read_bytes() == DATA


def test_fetch_starts_again_when_the_server_sends_it_all(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    (tmp_path / "m.gguf.part").write_bytes(DATA[:1000])
    fetch(Files({"m.gguf": DATA}, ranges=False).client(), f, tmp_path / "m.gguf")
    assert (tmp_path / "m.gguf").read_bytes() == DATA


def test_a_file_that_doesnt_match_is_deleted(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    with pytest.raises(DownloadError, match="checksum"):
        fetch(Files({"m.gguf": DATA[:-1] + b"x"}).client(), f, tmp_path / "m.gguf")
    assert not (tmp_path / "m.gguf").exists()
    assert not (tmp_path / "m.gguf.part").exists()


def test_cancelling_keeps_what_came_down(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    stop = threading.Event()

    def progress(done: int) -> None:
        if done > 0:
            stop.set()

    with pytest.raises(Cancelled):
        fetch(Files({"m.gguf": DATA}).client(), f, tmp_path / "m.gguf", progress, stop)
    assert 0 < (tmp_path / "m.gguf.part").stat().st_size < len(DATA)


def test_an_error_from_the_server(tmp_path: Path):
    f = file_of("m.gguf", DATA)
    client = httpx2.Client(transport=httpx2.MockTransport(lambda r: httpx2.Response(404)))
    with pytest.raises(DownloadError, match="404"):
        fetch(client, f, tmp_path / "m.gguf")


@pytest.fixture
def made_up(monkeypatch):
    """A catalog of one engine (a zip holding a program in a folder) and two models."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("build/llama-server.exe", b"program")
        z.writestr("build/ggml.dll", b"dll")
    zipped = buf.getvalue()
    eng = Engine("b1", file_of("engine.zip", zipped), "llama-server.exe")
    reader = Model(
        "reader",
        "Reader",
        (file_of("r.gguf", b"r" * 5000), file_of("mmproj-r.gguf", b"p" * 300)),
        frozenset({"vision", "chat"}),
        context=4096,
        memory_gb=1,
    )
    small = Model("small", "Small", (file_of("s.gguf", b"s" * 700),), frozenset({"chat"}), 2048, 1)
    monkeypatch.setattr(download, "engine", lambda: eng)
    monkeypatch.setattr(server, "engine", lambda: eng)
    models = {"reader": reader, "small": small}
    monkeypatch.setattr(catalog, "MODELS", models)
    monkeypatch.setattr(download, "MODELS", models)
    monkeypatch.setattr(server, "MODELS", models)
    monkeypatch.setattr(local_ai_api, "MODELS", models)
    monkeypatch.setattr(local_ai_api, "engine", lambda: eng)
    files = {f.name: data for f, data in ((eng.file, zipped),)}
    files |= {"r.gguf": b"r" * 5000, "mmproj-r.gguf": b"p" * 300, "s.gguf": b"s" * 700}
    return Files(files), eng, reader, small


def test_get_brings_the_engine_and_the_models(tmp_path: Path, made_up):
    files, eng, reader, _ = made_up
    seen = []
    download.get(
        tmp_path, ["reader"], lambda *a: seen.append(a), transport=httpx2.MockTransport(files)
    )
    assert eng.path(tmp_path).read_bytes() == b"program"
    assert (eng.folder(tmp_path) / "ggml.dll").exists()  # the folder, not just the program
    assert not (tmp_path / "engine" / "engine.zip").exists()
    assert reader.installed(tmp_path)
    total = eng.file.size + reader.size
    assert seen[-1] == ("Reader", total, total)
    assert {label for label, *_ in seen} == {"Lindley's AI engine", "Reader"}
    # Already there: nothing more to download
    assert download.wanted(tmp_path, ["reader"]) == []


def test_downloads_in_the_background(tmp_path: Path, made_up):
    files, _, reader, small = made_up
    d = Downloads(lambda: tmp_path, transport=httpx2.MockTransport(files))
    assert d.start(["reader", "small"]) == ["reader", "small"]
    assert d.idle.wait(10)
    assert reader.installed(tmp_path) and small.installed(tmp_path)
    assert activity.recent()[-1]["message"] == "Reader and Small downloaded, ready to use."
    assert d.asked() == []
    with pytest.raises(KeyError):
        d.start(["nothing"])


def test_a_failed_download_is_said(tmp_path: Path, made_up):
    files, *_ = made_up
    files.files["s.gguf"] = b"wrong"
    d = Downloads(lambda: tmp_path, transport=httpx2.MockTransport(files))
    d.start(["small"])
    assert d.idle.wait(10)
    last = activity.recent()[-1]
    assert last["kind"] == "download" and not last["ok"]
    assert last["message"].startswith("Couldn't download Small:")


def test_the_preset_names_each_model_and_its_files(tmp_path: Path, made_up):
    _, _, reader, small = made_up
    local = LocalAiSettings(models_dir=tmp_path, device="none")
    text = server.preset(local, [reader, small])
    assert "[reader]\nmodel = " in text and "mmproj = " in text
    assert str((tmp_path / "reader" / "mmproj-r.gguf").resolve()) in text
    assert "ctx-size = 4096" in text and "device = none\nn-gpu-layers = 0" in text
    assert "[small]" in text
    assert "device" not in server.preset(LocalAiSettings(models_dir=tmp_path), [small])


STUB = textwrap.dedent(
    """
    import http.server, sys
    port = int(sys.argv[sys.argv.index("--port") + 1])

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/health" else 404)
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')

        def log_message(self, *a):
            pass

    print("listening", " ".join(sys.argv[1:]), flush=True)
    http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
    """
)


@pytest.fixture
def stub(tmp_path: Path) -> list[str]:
    (tmp_path / "stub.py").write_text(STUB, encoding="utf-8")
    return [sys.executable, str(tmp_path / "stub.py")]


def install(root: Path, model: Model) -> None:
    for f in model.files:
        (model.folder(root) / f.name).parent.mkdir(parents=True, exist_ok=True)
        (model.folder(root) / f.name).write_bytes(b"x")


def test_the_server_starts_when_a_model_is_needed(tmp_path: Path, made_up, stub):
    _, _, reader, small = made_up
    root = tmp_path / "models"
    s = server.LocalServer(LocalAiSettings(models_dir=root), command=stub)
    with pytest.raises(ProviderError, match="Reader isn't downloaded yet"):
        s.url("reader")
    assert not s.running()
    install(root, reader)
    try:
        url = s.url("reader")
        assert url == f"http://127.0.0.1:{s.port}/v1" and s.running()
        assert "--offline" in s.log.read_text() and s.log.parent == root / "logs"
        assert s.url("reader") == url  # running: the same one
        # A model downloaded since: started again, knowing it
        install(root, small)
        s.url("small")
        assert "[small]" in s.log.with_suffix(".ini").read_text()
        with pytest.raises(ProviderError, match="no model called 'other'"):
            s.url("other")
    finally:
        s.stop()
    assert not s.running()


def test_a_server_that_wont_start(tmp_path: Path, made_up):
    _, _, reader, _ = made_up
    root = tmp_path / "models"
    install(root, reader)
    s = server.LocalServer(
        LocalAiSettings(models_dir=root), command=[sys.executable, "-c", "print('no Vulkan')"]
    )
    with pytest.raises(ProviderError, match="stopped as it started: no Vulkan"):
        s.url("reader")
    # Not downloaded at all: said in words
    s = server.LocalServer(LocalAiSettings(models_dir=root))
    with pytest.raises(ProviderError, match="isn't downloaded yet"):
        s.url("reader")


def test_one_server_for_the_settings_in_use(tmp_path: Path):
    a = server.use(LocalAiSettings(models_dir=tmp_path / "a"))
    assert server.use(LocalAiSettings(models_dir=tmp_path / "a")) is a
    b = server.use(LocalAiSettings(models_dir=tmp_path / "b"))
    assert b is not a
    server.stop()


def test_the_real_catalog_is_pinned():
    for m in catalog.MODELS.values():
        for f in m.files:
            assert f.url.startswith("https://huggingface.co/") and "/resolve/" in f.url
            assert len(f.sha256) == 64 and f.size > 0
            assert f.url.split("/resolve/")[1].split("/")[0] != "main"  # a fixed revision
        if "embed" in m.jobs:  # a page is embedded in one batch: it must hold the context
            assert int(m.preset["ubatch-size"]) >= m.context
    e = catalog.ENGINES["win32"]
    assert e.file.url.startswith("https://github.com/ggml-org/llama.cpp/releases/download/")


# ------------------------------------------------------------------ the API


def test_the_app_says_whats_downloaded(client, settings, made_up):
    _, eng, reader, _ = made_up
    s = client.get("/api/local-ai").json()
    assert s["folder"] == str(settings.ai.local.folder())
    assert s["engine"] == {"build": "b1", "size": eng.file.size, "ready": False}
    assert {m["id"]: m["state"] for m in s["models"]} == {"reader": "missing", "small": "missing"}
    assert s["free"] > 0 and s["downloading"] is None and s["running"] is False
    install(settings.ai.local.folder(), reader)
    s = client.get("/api/local-ai").json()
    assert s["models"][0]["state"] == "ready"


def test_downloads_are_asked_for_in_the_app(client, made_up):
    files, *_ = made_up
    client.app.state.downloads._transport = httpx2.MockTransport(files)
    r = client.post("/api/local-ai/download", json={"models": ["small"]})
    assert r.json() == {"queued": ["small"]}
    assert client.app.state.downloads.idle.wait(10)
    s = client.get("/api/local-ai").json()
    assert s["engine"]["ready"] and s["models"][1]["state"] == "ready"
    assert client.post("/api/local-ai/download", json={"models": ["huge"]}).status_code == 404
    assert client.post("/api/local-ai/cancel").json() == {"ok": True}


def test_a_model_in_use_isnt_removed(client, settings, made_up):
    _, _, reader, small = made_up
    root = settings.ai.local.folder()
    install(root, reader)
    install(root, small)
    current = client.get("/api/settings").json()
    current["ai"]["providers"]["own"] = {"type": "builtin"}
    current["ai"]["jobs"]["chat"] = {"connection": "own", "model": "reader"}
    client.put("/api/settings", json=current)
    r = client.delete("/api/local-ai/models/reader")
    assert r.status_code == 409 and "Reader is in use (chat)" in r.text
    assert client.delete("/api/local-ai/models/small").json() == {"removed": "small"}
    assert not small.folder(root).exists() and reader.installed(root)
    assert client.delete("/api/local-ai/models/nothing").status_code == 404


LOADING_STUB = textwrap.dedent(
    """
    import http.server, json, sys
    port = int(sys.argv[sys.argv.index("--port") + 1])
    ini = open(sys.argv[sys.argv.index("--models-preset") + 1]).read()
    names = [l.strip("[]") for l in ini.splitlines() if l.startswith("[")]
    on_graphics = "device = none" not in ini  # the graphics fail, the processor works
    asked = set()

    class H(http.server.BaseHTTPRequestHandler):
        def reply(self, body):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_GET(self):
            if self.path == "/health":
                return self.reply({"status": "ok"})
            status = lambda n: (
                {"value": "unloaded", "failed": True} if on_graphics
                else {"value": "loaded"} if n in asked else {"value": "unloaded"}
            )
            self.reply({"data": [{"id": n, "status": status(n)} for n in names]})

        def do_POST(self):
            n = self.rfile.read(int(self.headers["Content-Length"]))
            asked.add(json.loads(n)["model"])
            self.reply({"success": True})

        def log_message(self, *a):
            pass

    http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
    """
)


def test_graphics_that_cant_load_a_model_are_left_for_the_processor(tmp_path, made_up):
    _, _, reader, _ = made_up
    root = tmp_path / "models"
    install(root, reader)
    (tmp_path / "loading.py").write_text(LOADING_STUB, encoding="utf-8")
    command = [sys.executable, str(tmp_path / "loading.py")]
    s = server.LocalServer(LocalAiSettings(models_dir=root), command=command, loads=True)
    try:
        s.url("reader")
        assert s.on_processor and "device = none" in s.log.with_suffix(".ini").read_text()
    finally:
        s.stop()
    # A device a person chose is kept: its failure is said
    s = server.LocalServer(
        LocalAiSettings(models_dir=root, device="Vulkan1"), command=command, loads=True
    )
    try:
        with pytest.raises(ProviderError, match="Reader couldn't be loaded"):
            s.url("reader")
        assert not s.on_processor
    finally:
        s.stop()
