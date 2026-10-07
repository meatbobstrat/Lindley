"""The app as a whole: the built frontend it serves, and who may talk to it."""

from fastapi.testclient import TestClient

from lindley import app as app_mod


def test_the_frontends_own_pages_open_directly(settings, tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>Lindley</html>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("// app", encoding="utf-8")
    # Not installed: no lindley/web, so the repo's frontend/dist
    monkeypatch.setattr(app_mod, "FRONTEND_DIRS", (tmp_path / "web", dist))
    app = app_mod.create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert client.get("/").text == "<html>Lindley</html>"
        # Opened directly, or reloaded: the frontend finds the page in the browser
        for page in ("/inbox", "/documents/5", "/settings/ai"):
            r = client.get(page)
            assert r.status_code == 200 and r.text == "<html>Lindley</html>", page
        assert client.get("/assets/app.js").text == "// app"
        assert client.get("/assets/gone.js").status_code == 404
        assert client.get("/api/nothing-here").status_code == 404


def test_installed_its_own_copy_of_the_frontend_comes_first(settings, tmp_path, monkeypatch):
    for name in ("web", "dist"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "index.html").write_text(name, encoding="utf-8")
    monkeypatch.setattr(app_mod, "FRONTEND_DIRS", (tmp_path / "web", tmp_path / "dist"))
    app = app_mod.create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert client.get("/").text == "web"


def test_only_lindleys_own_names_reach_it(client):
    assert client.get("/api/health").status_code == 200
    for name in ("localhost:8765", "127.0.0.1:8765"):
        assert client.get("/api/health", headers={"host": name}).status_code == 200
    # A web page pointing a name of its own at this computer (DNS rebinding) is turned away
    assert client.get("/api/health", headers={"host": "evil.example:8765"}).status_code == 400


def test_listening_on_a_network_any_name_reaches_it():
    assert app_mod.allowed_hosts("0.0.0.0") == ["*"]
    assert app_mod.allowed_hosts("127.0.0.1") == ["127.0.0.1", "localhost"]


def test_another_site_cant_make_changes(client):
    other = {"origin": "https://evil.example"}
    assert client.post("/api/undo", headers=other).status_code == 403
    assert client.put("/api/pages/1/text", json={}, headers=other).status_code == 403
    assert client.get("/api/health", headers=other).status_code == 200  # reading is fine
    # Lindley's own pages: served by itself, or by the dev server; and scripts, with no Origin
    for own in ("http://127.0.0.1", "http://localhost:5173"):
        assert client.post("/api/undo", headers={"origin": own}).status_code != 403
    assert client.post("/api/undo").status_code != 403
