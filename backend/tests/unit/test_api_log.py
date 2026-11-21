"""The browser's errors, written to the log."""

import logging

import pytest

from lindley.api import log as log_api


@pytest.fixture(autouse=True)
def fresh_limit(monkeypatch):
    monkeypatch.setattr(log_api, "limit", log_api.Limit(3, 60))


def report(client, **body):
    return client.post("/api/log", json={"message": "x", **body})


def test_a_pages_error_is_in_the_log_with_where_and_its_stack(client, caplog):
    r = report(
        client,
        message="TypeError: x is undefined",
        where="/documents/4",
        stack="at render (app.js:10)\nat run (app.js:20)",
    )
    assert r.status_code == 204
    [rec] = [r for r in caplog.records if r.name == "lindley.frontend"]
    assert rec.levelno == logging.ERROR
    text = rec.getMessage()
    assert text.startswith("TypeError: x is undefined (on /documents/4)")
    assert "\n    at render (app.js:10)\n    at run (app.js:20)" in text


def test_a_warning_is_a_warning(client, caplog):
    report(client, level="warning", message="a request failed")
    [rec] = [r for r in caplog.records if r.name == "lindley.frontend"]
    assert rec.levelno == logging.WARNING


def test_keys_are_hidden_and_line_breaks_cant_forge_a_log_line(client, caplog):
    report(
        client,
        message="failed\nERROR lindley.fake: forged with sk-ant-api03-AbCdEfGhIjKlMnOp",
        where="/a\nb",
        stack="at x\nAuthorization: Bearer abcdef1234567890",
    )
    [rec] = [r for r in caplog.records if r.name == "lindley.frontend"]
    text = rec.getMessage()
    assert "AbCdEf" not in text and "abcdef1234567890" not in text
    first = text.split("\n")[0]
    assert "forged" in first  # on the one line, so it isn't a line of its own in the file
    assert all(line.startswith("    ") for line in text.split("\n")[1:])


def test_too_long_or_empty_reports_are_refused(client):
    assert report(client, message="m" * 501).status_code == 422
    assert report(client, message="").status_code == 422
    assert report(client, message="ok", stack="s" * 4001).status_code == 422
    assert report(client, level="fatal").status_code == 422


def test_a_page_that_reports_nonstop_cant_fill_the_log(client, caplog):
    codes = [report(client, message=f"error {i}").status_code for i in range(6)]
    assert codes == [204, 204, 204, 429, 429, 429]
    kept = [r for r in caplog.records if r.name == "lindley.frontend"]
    assert len([r for r in kept if r.levelno == logging.ERROR]) == 3
    said = [r for r in kept if "faster than 3 a minute" in r.getMessage()]
    assert len(said) == 1  # once, not for each one dropped


def test_only_lindleys_own_pages_may_report(client):
    r = client.post("/api/log", json={"message": "x"}, headers={"Origin": "http://evil.example"})
    assert r.status_code == 403
