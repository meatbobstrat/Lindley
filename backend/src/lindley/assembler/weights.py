"""Weights for the evidence in lindley.assembler.evidence, in log-odds.

Set by hand and checked on both benches (scripts/bench_assembler.py, made-up batches and real
scans). scripts/fit_assembler.py can fit them to pages whose right answer is known; with the
real scans so far (7 documents) fitted weights predicted single pairs better but built worse
documents, so these stay until there's more to learn from.

What the scanner saw (a long pause, a sheet of another size, other scan settings, handwriting
beside typing) is measured but weighed at nothing: the real scans so far were all scanned alike,
so there's nothing yet to set those weights by. A word split over a page break is set by hand.

The folder weights were fitted (scripts/fit_assembler.py --only) to 25 folders a person sorted,
filed each way people file scans, and came out near decisive: +4.8 for a shared folder, -6.7 for
different ones. But in every sorted folder so far a folder was one document, and a folder can
hold two, so they're set lower by hand, where a letter's greeting still splits a folder; the
fitted ones made wrong documents where some documents had folders and some didn't.
"""

WEIGHTS = {
    "bias": -0.85,
    "adjacent": 1.34,
    "starts": -2.0,
    "ends": -1.8,
    "kind_clash": -1.5,
    "number_next": 3.0,
    "number_near": 0.0,
    "number_far": 0.0,
    "runs_on": 1.95,
    "runs_on_apart": 1.26,
    "a_ends_mid": 0.35,
    "b_starts_mid": 0.35,
    "letterhead": 0.7,
    "names": 0.8,
    "paper_same": 0.25,
    "paper_differs": 0.0,
    "diary": 0.9,
    "layout_alike": 0.0,
    "layout_differs": -3.0,
    "words_shared": 0.0,
    "word_split": 1.0,
    "pause_long": 0.0,
    "size_differs": 0.0,
    "settings_differ": 0.0,
    "script_differs": 0.0,
    "topic_alike": 0.0,
    "folder_shared": 2.0,
    "folder_differs": -3.0,
    # A local model's log-odds that the writing runs on, asked only about pairs the rules are
    # unsure of (continues.py). Its chance is fairly well calibrated, so near 1: on the bench,
    # the rules' log-odds and the model's added ranked pairs best (design/database.md)
    "lm_continues": 0.8,
}

# A group's confidence, in log-odds, over segment.GROUP_FEATURES. Empty: the hand-made rule in
# segment._confidence. scripts/fit_confidence.py fits them.
GROUP_WEIGHTS: dict[str, float] = {}
