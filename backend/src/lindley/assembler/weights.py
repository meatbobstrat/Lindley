"""Weights for the evidence in lindley.assembler.evidence, in log-odds.

Set by hand and checked on both benches (scripts/bench_assembler.py, made-up batches and real
scans). scripts/fit_assembler.py can fit them to pages whose right answer is known; with the
real scans so far (7 documents) fitted weights predicted single pairs better but built worse
documents, so these stay until there's more to learn from.

What the scanner saw (a long pause, a sheet of another size, other scan settings, handwriting
beside typing) is measured but weighed at nothing: the real scans so far were all scanned alike,
so there's nothing yet to set those weights by. A word split over a page break is set by hand.
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
}

# A group's confidence, in log-odds, over segment.GROUP_FEATURES. Empty: the hand-made rule in
# segment._confidence. scripts/fit_assembler.py --groups fits them.
GROUP_WEIGHTS: dict[str, float] = {}
