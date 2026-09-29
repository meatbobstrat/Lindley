from pathlib import Path

import pytest

from lindley.config import (
    SETTINGS_ENV_VAR,
    ProviderConfig,
    Settings,
    load_settings,
    resolve_settings_path,
    save_settings,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_missing_file_gives_defaults(tmp_path: Path):
    s = load_settings(tmp_path / "nope.json")
    assert s == Settings()
    assert s.move_files is False
    assert s.ocr.engine == "hybrid"


def test_round_trip(tmp_path: Path):
    path = tmp_path / "settings.json"
    s = Settings(move_files=True, watch_folders=[Path("a"), Path("b")])
    save_settings(s, path)
    assert load_settings(path) == s


def test_example_settings_file_is_valid():
    s = load_settings(REPO_ROOT / "settings.example.json")
    assert s.ai.chat_provider in s.ai.providers
    assert s.ocr.vision_provider in s.ai.providers


def test_env_var_overrides_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    target = tmp_path / "custom.json"
    monkeypatch.setenv(SETTINGS_ENV_VAR, str(target))
    assert resolve_settings_path() == target
    assert resolve_settings_path(tmp_path / "explicit.json") == tmp_path / "explicit.json"


def test_api_key_read_from_env(monkeypatch: pytest.MonkeyPatch):
    cfg = ProviderConfig(type="anthropic", api_key_env="LINDLEY_TEST_KEY")
    monkeypatch.setenv("LINDLEY_TEST_KEY", "secret")
    assert cfg.api_key() == "secret"
    assert ProviderConfig(type="fake").api_key() is None
