"""Lindley's logging: what's kept, what's left out, and that no key reaches it."""

import logging
import logging.config
import sys
import threading

from lindley import logs


def _record(msg, *args, name="lindley.x", level=logging.INFO, exc_info=None):
    return logging.LogRecord(name, level, __file__, 1, msg, args or None, exc_info)


def test_keys_and_tokens_are_hidden():
    for secret in (
        "sk-ant-api03-AbCdEfGhIjKlMnOp",
        "sk-proj-1234567890abcdef",
        "AIzaSyA1234567890abcdefghijklmnop",
    ):
        assert secret not in logs.redact(f"The key {secret} was refused")
    assert "abcdef123456" not in logs.redact("Authorization: Bearer abcdef123456")
    assert "abcdef123456" not in logs.redact("headers {'x-api-key': 'abcdef123456'}")
    assert "abcdef123456" not in logs.redact("GET http://x/v1?key=abcdef123456&alt=json")
    assert "hunter2" not in logs.redact("api_key=hunter2")
    plain = "Page 4 of 12 read in 3.2 s (key stage 2)"
    assert logs.redact(plain) == plain  # ordinary words are left


def test_the_filter_hides_a_key_in_the_message_and_in_a_traceback():
    f = logs.PrivacyFilter()
    r = _record("Calling with %s", "sk-ant-api03-AbCdEfGhIjKlMnOp")
    assert f.filter(r) and "AbCdEf" not in r.getMessage()
    try:
        raise ValueError("bad key sk-ant-api03-AbCdEfGhIjKlMnOp")
    except ValueError:
        r = _record("failed", exc_info=sys.exc_info())
    assert f.filter(r) and "AbCdEf" not in r.exc_text


def test_chatty_libraries_are_held_to_warnings():
    f = logs.PrivacyFilter()
    assert not f.filter(_record("Request options: {...}", name="openai._base_client", level=10))
    assert not f.filter(_record("send_request_body", name="httpcore.http11", level=10))
    assert f.filter(_record("rate limited", name="openai._base_client", level=logging.WARNING))
    assert f.filter(_record("fine", name="lindley.pipeline", level=10))


def test_web_address_shows_where_and_nothing_else():
    assert logs.web_address("https://user:pw@host.example:8443/v1/x?key=abc") == (
        "https://host.example:8443"
    )
    assert logs.web_address("http://localhost:11434/v1") == "http://localhost:11434"
    assert logs.web_address("") == "" and logs.web_address("http://h:notaport/") == ""


def test_the_screens_regular_questions_stay_out_of_the_access_log():
    f = logs.PollingFilter()

    def access(method, path, status):
        return _record('%s - "%s %s HTTP/%s" %d', "127.0.0.1:5", method, path, "1.1", status)

    assert not f.filter(access("GET", "/api/overview", 200))
    assert f.filter(access("GET", "/api/overview", 500))  # a failure is wanted
    assert f.filter(access("POST", "/api/documents", 200))
    assert f.filter(access("GET", "/api/pages/3", 200))


def test_the_level_comes_from_the_flag_then_the_environment():
    assert logs.level_from(True, {}) == "DEBUG"
    assert logs.level_from(False, {logs.LEVEL_ENV_VAR: " warn "}) == "WARNING"
    assert logs.level_from(False, {logs.LEVEL_ENV_VAR: "debug"}) == "DEBUG"
    assert logs.level_from(False, {logs.LEVEL_ENV_VAR: "loud"}) == "INFO"
    assert logs.level_from(True, {logs.LEVEL_ENV_VAR: "ERROR"}) == "DEBUG"  # the flag wins
    assert logs.level_from(False, {}) == "INFO"


def test_the_config_keeps_everything_in_the_file_and_is_applied_cleanly(tmp_path):
    file = tmp_path / "logs" / "lindley.log"
    config = logs.log_config(file, "DEBUG")
    assert config["root"]["level"] == "WARNING" and "file" in config["root"]["handlers"]
    assert all(config["loggers"][n]["level"] == "WARNING" for n in logs.QUIET)
    assert "filters" not in config["loggers"]["uvicorn.access"]  # debug: the polling too
    assert config["handlers"]["file"]["maxBytes"] == logs.LOG_BYTES
    assert "filters" in logs.log_config(file, "INFO")["loggers"]["uvicorn.access"]
    assert "file" not in logs.log_config(None)["handlers"]  # no file: console only
    assert logs.log_config(None)["root"]["handlers"] == ["default"]

    root, lindley = logging.getLogger(), logging.getLogger("lindley")
    before = (root.handlers[:], root.level, lindley.handlers[:], lindley.level, lindley.propagate)
    try:
        logging.config.dictConfig(config)  # it must be one dictConfig accepts
        logging.getLogger("lindley.test").debug("detail %s", "sk-ant-api03-AbCdEfGhIjKlMnOp")
        logging.getLogger("some.library").warning("seen")
        logging.getLogger("openai").info("a whole request")
        for h in lindley.handlers + root.handlers:
            h.flush()
        text = file.read_text(encoding="utf-8")
    finally:
        for h in lindley.handlers + root.handlers:
            if isinstance(h, logging.FileHandler):
                h.close()
        root.handlers[:], root.level = before[0], before[1]
        lindley.handlers[:], lindley.level, lindley.propagate = before[2:]
    assert "DEBUG lindley.test: detail sk-***" in text  # the debug line, key hidden
    assert "some.library: seen" in text  # a library's warning isn't lost
    assert "a whole request" not in text and "AbCdEf" not in text


def test_what_nothing_caught_is_logged_with_its_traceback(caplog, monkeypatch):
    monkeypatch.setattr(sys, "excepthook", sys.excepthook)  # put back after the test
    monkeypatch.setattr(threading, "excepthook", threading.excepthook)
    logs.install_excepthooks()

    def fail():
        raise RuntimeError("thread blew up")

    with caplog.at_level(logging.CRITICAL, logger="lindley"):
        t = threading.Thread(target=fail, name="lindley-test")
        t.start()
        t.join()
        try:
            raise KeyError("main blew up")
        except KeyError:
            sys.excepthook(*sys.exc_info())
    text = caplog.text
    assert "lindley-test" in text and "thread blew up" in text and "main blew up" in text
