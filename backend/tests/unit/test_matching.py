"""Layout fingerprints, shared rare words, the evidence model and fitting it."""

from lindley.assembler import evidence
from lindley.assembler.clues import Line
from lindley.assembler.evidence import FEATURES, pair
from lindley.assembler.layout import page_layout
from lindley.assembler.learn import Example, accuracy, fit
from lindley.assembler.model import Page, weigh_terms
from lindley.assembler.terms import Library, overlap, page_terms, weigh

WORDS = ["the", "quick", "brown", "fox", "jumps", "over", "lazy", "dogs", "while", "seven"]


def typed(lines=12, left=600, spacing=100, char=20, width=2550, chars=60):
    """Word boxes for a typed page: `lines` lines of about `chars` characters each."""
    out, y = [], 800
    for _ in range(lines):
        x, row, n = left, [], 0
        for word in WORDS * 3:
            if n + len(word) > chars:
                break
            row.append({"text": word, "conf": 92.0, "bbox": [x, y, char * len(word), 30]})
            x += char * (len(word) + 1)
            n += len(word) + 1
        out.append(Line(" ".join(w["text"] for w in row), row))
        y += spacing
    return out, width


def test_a_layout_is_the_same_whatever_the_scan_resolution():
    a = page_layout(*typed())
    b = page_layout(*typed(left=300, spacing=50, char=10, width=1275))  # the page at half size
    assert a and b and a.differs(b) < 0.1
    assert round(a.spacing, 1) == 5.0 and 55 <= a.line_chars <= 62


def test_pages_set_out_differently_have_different_layouts():
    double = page_layout(*typed(spacing=100))
    single = page_layout(*typed(spacing=50))
    narrow = page_layout(*typed(chars=35))
    assert double.differs(single) > 2 and double.differs(narrow) > 2


def test_too_little_writing_has_no_layout():
    assert page_layout(*typed(lines=3)) is None
    assert page_layout(typed()[0], None) is None


def test_rare_words_two_pages_share_count_and_common_ones_dont():
    texts = [
        "The Tonopah mine was sold to Goldfield men",
        "Tonopah and Goldfield grew fast that year",
        "The river rose and the mine flooded",
        "Mother wrote about the river and the garden",
        "The garden was full of roses",
    ]
    v = weigh([page_terms(t) for t in texts])
    likeness, words = overlap(v[0], v[1])
    assert likeness > 0.5 and set(words) == {"tonopah", "goldfield"}
    assert overlap(v[0], v[4])[0] == 0


def page(pid, text, words=None, width=None, file_name=None):
    return Page(
        pid, pid, file_name or f"scan_{pid:04d}.jpg", text, words=words, height=3300, width=width
    )


def test_pages_set_out_differently_are_kept_apart():
    double, width = typed(spacing=100)
    single, _ = typed(spacing=50)
    text = "\n".join(ln.text for ln in double)
    a = page(1, text, [w for ln in double for w in ln.words], width)
    b = page(2, text, [w for ln in single for w in ln.words], width)
    c = page(3, text, [w for ln in double for w in ln.words], width)
    assert pair(a, b).features["layout_differs"] == 1.0
    assert "they're set out differently on the page" in pair(a, b).breaks
    assert pair(a, c, True).score > pair(a, b, True).score + 0.3


def test_evidence_weighed_at_nothing_is_never_given_as_a_reason(monkeypatch):
    a = page(1, "We went to Tonopah and Goldfield in the spring")
    b = page(2, "Tonopah and Goldfield were booming that summer")
    c = page(3, "The garden is full of roses")
    weigh_terms([a, b, c])
    assert pair(a, b).features["words_shared"] > 0
    monkeypatch.setitem(evidence.WEIGHTS, "words_shared", 0.0)
    assert not any("words" in k.note for k in pair(a, b).links)
    monkeypatch.setitem(evidence.WEIGHTS, "words_shared", 1.0)
    assert any("Tonopah" in k.note or "tonopah" in k.note for k in pair(a, b).links)


def example(**on):
    x = [1.0 if k == "bias" else float(on.get(k, 0.0)) for k in FEATURES]
    return x


def test_fitting_learns_which_evidence_points_which_way():
    data = []
    for i in range(200):
        runs, starts = i % 2 == 0, i % 3 == 0
        same = runs and not starts
        data.append(Example(example(runs_on=runs, starts=starts), int(same)))
    w = fit(data)
    assert w["runs_on"] > 1 and w["starts"] < -1
    assert accuracy(data, w) == 1.0


def test_fitting_around_fixed_weights_leaves_them_alone():
    data = [Example(example(runs_on=i % 2), i % 2) for i in range(100)]
    w = fit(data, fixed={"bias": -0.85, "runs_on": 0.5})
    assert w["bias"] == -0.85 and w["runs_on"] == 0.5


def test_rarity_is_measured_over_the_whole_library():
    texts = ["Tonopah mining news", "Tonopah stage today"]
    alone = weigh([page_terms(t) for t in texts])
    assert not alone[0].get("tonopah")  # on every page there is: no weight
    library = Library.of(texts + [f"Page {i} of a farm diary" for i in range(20)])
    assert weigh([page_terms(t) for t in texts], library)[0]["tonopah"] > 0


def _sheet(i, text, **kw):
    return Page(i, i, f"scan_{i:04d}.jpg", text, **kw)


BODY = "The road ran north over the summit and down the long grade toward the camp"


def test_a_word_split_over_the_page_break_runs_on_even_read_as_equals():
    a = _sheet(1, BODY + "\nand the wagons came over the two=by=")
    b = _sheet(2, "four bridge at noon, and went on to the camp\n" + BODY)
    f = pair(a, b).features
    assert f["word_split"] == 1.0 and f["runs_on"] == 1.0
    assert pair(_sheet(3, BODY + "\nand the wagons came over"), b).features["word_split"] == 0


def test_what_the_scanner_saw_is_evidence():
    letter = {"dpi": 300, "width": 2550, "height": 3300, "color_mode": "rgb"}
    a = _sheet(1, BODY, script="typed", scanned_at="1999-01-01 10:00:00", **letter)
    same = _sheet(2, BODY, script="typed", scanned_at="1999-01-01 10:01:00", **letter)
    assert not any(
        pair(a, same).features[k]
        for k in ("pause_long", "size_differs", "settings_differ", "script_differs")
    )
    half = {**letter, "height": 1650, "color_mode": "gray"}
    other = _sheet(2, BODY, script="handwritten", modified_at="1999-01-01 10:30:00", **half)
    f = pair(a, other).features
    assert f["pause_long"] == f["size_differs"] == f["settings_differ"] == 1.0
    assert f["script_differs"] == 1.0
    assert "the sheets are different sizes" in pair(a, other).breaks


def test_a_fitted_group_confidence_is_used_when_there_is_one(monkeypatch):
    from lindley.assembler import segment

    pages = [_sheet(1, "Dear Sister,\n" + BODY), _sheet(2, BODY + "\nYour loving brother\nWill")]
    weigh_terms(pages)
    rule = segment.segment(pages)[0][0]
    assert rule.features["start"] == rule.features["end"] == 1.0
    monkeypatch.setattr(segment, "GROUP_WEIGHTS", {"bias": -5.0})
    assert segment.segment(pages)[0][0].confidence == 1  # 1 / (1 + e^5), as a percentage
    assert rule.confidence > 1
