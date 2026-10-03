import io

import pytest
from PIL import Image, ImageDraw

from lindley.config import Settings
from lindley.db.database import connect, init_db
from lindley.providers.base import Transcription
from lindley.worker.intake import import_file
from lindley.worker.ocr.base import PageResult
from lindley.worker.ocr.vision import VisionEngine, image_bytes
from lindley.worker.pipeline import Pipeline


class Recording:
    """A vision model that keeps what it was sent."""

    model = "recording"

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def transcribe(self, image, hints=None):
        self.sent.append(image)
        return Transcription(text="Dear Sister", confidence=95.0)


class HardToRead:
    """Tesseract struggling with a page."""

    name = "tesseract"
    version = "tesseract v5.test eng"

    def recognize(self, image_path):
        return [PageResult(1, "Dcar", 20.0, self.name, [])]


@pytest.fixture
def conn(settings: Settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


def opened(data: bytes) -> Image.Image:
    return Image.open(io.BytesIO(data))


def test_a_big_scan_is_reduced_before_it_is_sent(tmp_path):
    path = tmp_path / "big.png"
    Image.new("RGB", (5100, 6600), "white").save(path)  # letter size at 600 dpi
    vision = Recording()
    VisionEngine(vision, max_side=2000).recognize(path)
    img = opened(vision.sent[0])
    assert img.format == "JPEG" and img.size == (1545, 2000)


def test_a_small_page_is_sent_as_it_is(tmp_path):
    path = tmp_path / "small.png"
    Image.new("RGB", (800, 1100), "white").save(path)
    assert image_bytes(path, 2000) == path.read_bytes()


def test_a_format_providers_dont_take_is_converted(tmp_path):
    path = tmp_path / "scan.tif"
    Image.new("1", (800, 1100), 1).save(path)  # bilevel TIFF, as many scanners make
    img = opened(image_bytes(path, 2000))
    assert img.format == "JPEG" and img.size == (800, 1100)


def test_a_heavy_file_is_re_encoded_even_if_it_fits(tmp_path):
    path = tmp_path / "noisy.png"
    Image.effect_noise((1900, 1900), 120).convert("RGB").save(path)  # noise compresses badly
    assert path.stat().st_size > 4_000_000
    assert len(image_bytes(path, 2000)) < path.stat().st_size


def test_the_pipeline_uses_the_size_in_settings(conn, settings, tmp_path):
    settings.ocr.vision_mode, settings.ocr.vision_max_side = "auto", 600
    img = Image.new("RGB", (1200, 1600), "white")
    ImageDraw.Draw(img).rectangle([100, 100, 1000, 140], fill="black")
    img.save(tmp_path / "scan_0001.png")
    sid = import_file(conn, settings, tmp_path / "scan_0001.png").scan_id
    vision = Recording()
    assert Pipeline(settings, HardToRead(), vision).process_scan(conn, sid) == "read"
    assert max(opened(vision.sent[0]).size) == 600
