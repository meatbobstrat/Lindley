"""Weights for the evidence in lindley.assembler.evidence, in log-odds.

Set by hand and checked on both benches (scripts/bench_assembler.py, made-up batches and real
scans). scripts/fit_assembler.py can fit them to pages whose right answer is known; with the
real scans so far (7 documents) fitted weights predicted single pairs better but built worse
documents, so these stay until there's more to learn from.
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
}
