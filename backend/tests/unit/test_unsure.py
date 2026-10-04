"""Words a reader wasn't sure of, for the UI to highlight."""

from lindley.worker.ocr.base import UNSURE_BELOW, PageResult, marked_confidence, unsure_spans


def word(text, conf):
    return {"text": text, "conf": conf, "bbox": [0, 0, 1, 1]}


def test_words_tesseract_was_unsure_of_are_found_in_its_text():
    text = "Dear John,\nthe river was up"
    words = [word("Dear", 95), word("John,", 40), word("the", 96), word("river", UNSURE_BELOW)]
    words += [word("was", 12), word("up", 90)]
    assert unsure_spans(text, words) == [[5, 10], [21, 24]]
    assert [text[a:b] for a, b in unsure_spans(text, words)] == ["John,", "was"]


def test_words_the_vision_model_marked_are_found():
    text = "We went to Xenia [?] on the [illegible] and came home."
    assert [text[a:b] for a, b in unsure_spans(text)] == ["Xenia", "[illegible]"]


def test_a_sure_reading_has_none():
    assert PageResult(1, "All clear", 97.0, "tesseract", [word("All", 97)]).unsure_spans() == []


def test_a_vision_readings_confidence_is_the_share_of_words_it_was_sure_of():
    assert marked_confidence("Dear Sister, we are well.") == 100.0
    assert marked_confidence("We went to Xenia [?] on the [illegible] day.") == 75.0
    assert marked_confidence(" ".join(["[illegible]"] * 730)) == 0.0
    assert marked_confidence("") is None and marked_confidence("  \n ") is None  # a blank page
