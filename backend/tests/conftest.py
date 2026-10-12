from pathlib import Path

import keyring
import pytest
from fastapi.testclient import TestClient
from keyring.backend import KeyringBackend
from keyring.errors import PasswordDeleteError

from lindley.app import create_app
from lindley.config import AiSettings, JobConfig, LocalAiSettings, ProviderConfig, Settings
from lindley.providers.base import JOBS


class MemoryKeyring(KeyringBackend):
    priority = 1

    def __init__(self) -> None:
        super().__init__()
        self.keys: dict[tuple[str, str], str] = {}

    def get_password(self, service, username):
        return self.keys.get((service, username))

    def set_password(self, service, username, password):
        self.keys[(service, username)] = password

    def delete_password(self, service, username):
        if self.keys.pop((service, username), None) is None:
            raise PasswordDeleteError(username)


@pytest.fixture(autouse=True)
def no_activity_left():
    """What the AI was doing is kept for the whole process: each test starts with none."""
    from lindley import activity

    activity.clear()
    yield
    activity.clear()


@pytest.fixture(autouse=True)
def no_real_folders(monkeypatch, tmp_path_factory):
    """Tests never write to the person's own folders: a setting a test leaves out (a new one,
    say) falls back to a temporary Documents and data folder, not the real ones."""
    home = tmp_path_factory.mktemp("home")
    monkeypatch.setattr("lindley.config.user_documents_dir", lambda: str(home / "Documents"))
    monkeypatch.setattr(
        "lindley.config.user_data_dir", lambda *a, **k: str(home / "AppData" / "Lindley")
    )
    monkeypatch.setattr(
        "lindley.config.user_config_dir", lambda *a, **k: str(home / "Config" / "Lindley")
    )


@pytest.fixture(autouse=True)
def keys():
    """Tests never touch the real credential store."""
    before = keyring.get_keyring()
    store = MemoryKeyring()
    keyring.set_keyring(store)
    yield store
    keyring.set_keyring(before)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        watch_folders=[tmp_path / "inbox"],
        processing_dir=tmp_path / "processing",
        quarantine_dir=tmp_path / "quarantine",
        library_dir=tmp_path / "library",
        output_dir=tmp_path / "exported",
        db_path=tmp_path / "lindley.db",
        # One AI connection, the fake one, doing every job; it asks first, as out of the box.
        # Not checking whether pages carry on (tests of that give it the job themselves).
        ai=AiSettings(
            providers={"local": ProviderConfig(type="fake")},
            jobs={job: JobConfig(connection="local") for job in JOBS if job != "continues"},
            local=LocalAiSettings(models_dir=tmp_path / "models"),
        ),
    )


@pytest.fixture
def client(settings: Settings, tmp_path: Path):
    app = create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app, base_url="http://127.0.0.1") as c:
        yield c
