from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lindley.app import create_app
from lindley.config import AiSettings, JobConfig, ProviderConfig, Settings
from lindley.providers.base import JOBS


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        watch_folders=[tmp_path / "inbox"],
        processing_dir=tmp_path / "processing",
        quarantine_dir=tmp_path / "quarantine",
        library_dir=tmp_path / "library",
        db_path=tmp_path / "lindley.db",
        # One AI connection, the fake one, doing every job; it asks first, as out of the box.
        ai=AiSettings(
            providers={"local": ProviderConfig(type="fake")},
            jobs={job: JobConfig(connection="local") for job in JOBS},
        ),
    )


@pytest.fixture
def client(settings: Settings, tmp_path: Path):
    app = create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app) as c:
        yield c
