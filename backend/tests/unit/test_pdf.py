import pypdfium2 as pdfium
import pytest
from PIL import Image, ImageDraw

from lindley.export.pdf import PdfInfo, PdfPage, build_pdf, plain
from lindley.export.textlayer import Word
from lindley.worker.image import EXIF_ORIENTATION

SIZE = (1275, 1650)  # US letter at 150 dpi


def upright(mode="RGB"):
    """A page with a dark mark at its top left, to tell which way up it came out."""
    img = Image.new(mode, SIZE, "white")
    ImageDraw.Draw(img).rectangle((50, 50, 250, 150), fill="black")
    return img


def save(img, path, **kw):
    img.save(path, dpi=(150, 150), **kw)
    return path


def words():
    return [
        Word("Dear", (300, 400, 160, 40)),
        Word("Sister,", (490, 400, 240, 48)),
        Word("Your", (300, 1200, 150, 40)),
        Word("son", (480, 1200, 110, 40)),
    ]


def read(data: bytes) -> pdfium.PdfDocument:
    return pdfium.PdfDocument(data)


def dark_corner(page: pdfium.PdfPage) -> bool:
    """Whether the mark is at the top left of the rendered page."""
    img = page.render(scale=0.25).to_pil().convert("L")
    w, h = img.size
    return img.getpixel((int(w * 0.1), int(h * 0.06))) < 100


def test_pages_carry_their_words_where_the_writing_is(tmp_path):
    path = save(upright(), tmp_path / "p.jpg", quality=90)
    doc = read(build_pdf([PdfPage(path, 0, 150, "rgb", words())], PdfInfo("Letter from Will")))
    assert len(doc) == 1
    page = doc[0]
    assert page.get_size() == pytest.approx((612, 792))
    text = page.get_textpage()
    assert text.get_text_range().split() == ["Dear", "Sister,", "Your", "son"]
    # "S" of Sister: inside its box, in points from the bottom left
    left, bottom, right, top = text.get_charbox(5)
    scale = 72 / 150
    assert 490 * scale - 2 <= left < right <= (490 + 240) * scale
    assert 792 - (400 + 48) * scale - 2 <= bottom < top <= 792 - 400 * scale + 2
    # the word is stretched to its box: the comma's ink ends just short of the box's right edge
    assert (490 + 240) * scale - 6 <= text.get_charbox(11)[2] <= (490 + 240) * scale + 1


def test_a_jpeg_that_needs_no_turning_is_embedded_as_it_is(tmp_path):
    path = save(upright(), tmp_path / "p.jpg", quality=90)
    data = build_pdf([PdfPage(path, 0, 150, "rgb", [])], PdfInfo("x"))
    assert b"/DCTDecode" in data
    assert len(data) < path.stat().st_size + 5000


def test_pages_come_out_upright(tmp_path):
    sideways = save(upright().rotate(90, expand=True), tmp_path / "turned.png")
    exif = Image.Exif()
    exif[EXIF_ORIENTATION] = 6  # shown turned a quarter clockwise
    tagged = save(upright().rotate(90, expand=True), tmp_path / "tagged.jpg", exif=exif)
    plain_page = save(upright(), tmp_path / "plain.png")
    data = build_pdf(
        [
            PdfPage(sideways, 90, 150, "rgb", []),
            PdfPage(tagged, 0, 150, "rgb", []),
            PdfPage(plain_page, 0, 150, "rgb", []),
        ],
        PdfInfo("x"),
    )
    doc = read(data)
    for page in doc:
        assert page.get_size() == pytest.approx((612, 792))
        assert dark_corner(page)


def test_bilevel_pages_stay_one_bit(tmp_path):
    path = save(upright("1"), tmp_path / "p.png")
    data = build_pdf([PdfPage(path, 0, 150, "bilevel", [])], PdfInfo("x"))
    assert b"/BitsPerComponent 1" in data
    assert dark_corner(read(data)[0])


def test_page_size_comes_from_the_dpi_or_a_sensible_guess(tmp_path):
    img = upright()
    a = save(img, tmp_path / "a.png")
    b = tmp_path / "b.png"
    img.save(b)  # no dpi
    doc = read(
        build_pdf(
            [
                PdfPage(a, 0, 150, "rgb", []),
                PdfPage(b, 0, None, "rgb", []),  # taken as 300 dpi: 4.25 x 5.5 in
                PdfPage(a, 0, 72, "rgb", []),  # a phone's 72 dpi: sized as 11 in long
            ],
            PdfInfo("x"),
        )
    )
    assert doc[0].get_size() == pytest.approx((612, 792))
    assert doc[1].get_size() == pytest.approx((306, 396))
    assert doc[2].get_size()[1] == pytest.approx(792)


def test_the_title_and_subject_are_saved(tmp_path):
    path = save(upright(), tmp_path / "p.png")
    doc = read(build_pdf([PdfPage(path, 0, 150, "rgb", [])], PdfInfo("Deed, 1892", "deed, 1892")))
    meta = doc.get_metadata_dict()
    assert meta["Title"] == "Deed, 1892"
    assert meta["Subject"] == "deed, 1892"
    assert meta["Creator"].startswith("Lindley")


def test_text_outside_windows_1252_is_kept_as_plain_letters():
    assert plain("café “quoted” em—dash") == "café “quoted” em—dash"
    assert plain("ſo") == "so"  # long s
    assert plain("Ωmega") == "?mega"


def test_words_with_other_letters_still_go_in(tmp_path):
    path = save(upright(), tmp_path / "p.png")
    page = PdfPage(
        path, 0, 150, "rgb", [Word("Zoë", (300, 400, 120, 40)), Word("ſo", (450, 400, 80, 40))]
    )
    text = read(build_pdf([page], PdfInfo("x")))[0].get_textpage().get_text_range()
    assert text.split() == ["Zoë", "so"]
