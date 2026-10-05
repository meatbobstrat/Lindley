"""The app as a whole: the built frontend it serves, and who may talk to it."""

from fastapi.testclient import TestClient

from lindley import app as app_mod


def test_the_frontends_own_pages_open_directly(settings, tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>Lindley</html>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("// app", encoding="utf-8")
    monkeypatch.setattr(app_mod, "FRONTEND_DIST", dist)
    app = app_mod.create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app) as client:
        assert client.get("/").text == "<html>Lindley</html>"
        # Opened directly, or reloaded: the frontend finds the page in the browser
        for page in ("/inbox", "/documents/5", "/settings/ai"):
            r = client.get(page)
            assert r.status_code == 200 and r.text == "<html>Lindley</html>", page
        assert client.get("/assets/app.js").text == "// app"
        assert client.get("/assets/gone.js").status_code == 404
        assert client.get("/api/nothing-here").status_code == 404
