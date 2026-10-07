"""Whether one page carries straight on from another, asked of a small local model (the continues
job), and how its answer weighs in sorting."""

import pytest

from lindley.assembler import assemble, continues
from lindley.assembler.answers import Answers
from lindley.assembler.auto import judge_for, sort_with
from lindley.assembler.bench import TruePage, load
from lindley.assembler.evidence import FEATURES, pair
from lindley.assembler.model import Page
from lindley.config import JobConfig, ProviderConfig
from lindley.db.database import connect, init_db
from lindley.providers import allowance
from lindley.providers.base import ProviderError

TYPESCRIPT = [
    "THE OLD MILL\nWe walked down to the mill in the spring of that year and found\nthe wheel"
    " stopped and the race full of leaves and the miller gone to",
    "town for the day, so we sat on the bank and ate our dinner and watched\nthe water go over"
    " the dam until it was nearly dark and time to go\nhome along the river road past the church"
    " and the",
    "school where Mother had taught before she was married, which was\nstill standing then though"
    " the roof had fallen in at one end and the\nbell was gone from the little tower over the"
    " door and",
    "Dear Mr. Hale,\nThank you for your letter of June 26.\nI am sorry to hear about the barn.\n"
    "Yours truly,\nJohn",
]


def scanned(names: list[str]) -> list[Page]:
    return [
        Page(i + 1, i + 1, f"{n}.jpg", t)
        for i, (n, t) in enumerate(zip(names, TYPESCRIPT, strict=False))
    ]


@pytest.fixture(autouse=True)
def shipped_weights():
    """Scores here are the shipped weights': a test before may have left learned ones in use."""
    from lindley.assembler import evidence

    evidence.use_weights(None)
    yield
    evidence.use_weights(None)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


class Judge:
    """Says yes to the pairs in `yes`, no to the rest, and keeps what it was asked."""

    def __init__(self, yes=(), fail: Exception | None = None) -> None:
        self.yes = set(yes)
        self.fail = fail
        self.asked: list[str] = []

    def p_yes(self, system: str, question: str) -> float:
        self.asked.append(question)
        if self.fail:
            raise self.fail
        return 0.95 if any(q in question for q in self.yes) else 0.05


def test_the_pairs_the_rules_may_get_wrong_are_asked_about():
    in_order = scanned(["Image (1)", "Image (2)", "Image (3)", "Image (4)"])
    # Scanned one after the other and running on, the rules are sure: not asked
    assert pair(in_order[0], in_order[1]).score > continues.UNSURE[1]
    asked = [(a.id, b.id) for a, b in continues.unsure_pairs(in_order)]
    assert (1, 2) not in asked and (2, 3) not in asked
    assert asked[0] == (3, 4)  # the typescript then a letter: unsure, and first
    # Scanned apart, each page's best few candidates near the bar for joining
    apart = scanned(["a", "q", "c", "z"])
    asked = continues.unsure_pairs(apart)
    assert {(1, 2), (1, 3), (2, 3), (3, 2)} <= {(a.id, b.id) for a, b in asked}
    for a, b in asked:
        assert pair(a, b).score >= continues.NEAR_JOIN
    assert len([a for a, _ in asked if a.id == 1]) <= continues.APART


def test_the_question_shows_the_end_of_one_and_the_start_of_the_next():
    a, b, *_ = scanned(["a", "b"])
    q = continues.question(a, b)
    assert q.startswith("Page A ends:\n") and "\n\nPage B starts:\ntown for the day" in q
    assert "THE OLD MILL" in q  # all of a short page


def test_a_yes_weighs_in_and_is_given_as_a_reason():
    a, b, *_ = scanned(["a", "q"])
    before = pair(a, b)
    assert before.features["lm_continues"] == 0 and "lm_continues" in FEATURES
    a.runs_into[b.id] = 0.95
    yes = pair(a, b)
    assert yes.score > before.score and yes.features["lm_continues"] == pytest.approx(2.944, 0.01)
    assert "The writing carries straight on, page to page" in [k.note for k in yes.links]
    a.runs_into[b.id] = 1.0  # held within 1% of sure
    assert pair(a, b).features["lm_continues"] == pytest.approx(4.595, 0.01)
    # Its no is measured apart, and weighed at nothing: it split too many pages that run on
    a.runs_into[b.id] = 0.05
    no = pair(a, b)
    assert no.features["lm_breaks"] == pytest.approx(2.944, 0.01)
    assert no.features["lm_continues"] == 0 and no.score == pytest.approx(before.score)
    assert "the writing doesn't carry on from one to the other" not in no.breaks


def test_answers_are_kept_and_never_asked_twice(conn):
    pages = scanned(["a", "q", "c", "z"])
    judge = Judge(yes=["Page B starts:\ntown for the day"])
    answers = Answers(conn)
    unsure = len(continues.unsure_pairs(pages))
    asked = continues.judge(pages, judge, answers)
    assert asked == len(judge.asked) == unsure
    assert pages[0].runs_into[2] == 0.95 and pages[0].runs_into[3] == 0.05
    assert answers.calls == 0 and answers.judged == asked  # not the sorting AI's calls
    # Sorted again: the same text is answered from what was kept
    again = scanned(["a", "q", "c", "z"])
    later = Judge()
    assert continues.judge(again, later, Answers(conn)) == 0 and later.asked == []
    assert again[0].runs_into[2] == 0.95
    # Without a judge, kept answers are still used
    alone = scanned(["a", "q", "c", "z"])
    continues.judge(alone, None, Answers(conn))
    assert alone[0].runs_into == pages[0].runs_into
    kept = conn.execute("SELECT purpose, page_ids FROM ai_answers").fetchall()
    assert {r[0] for r in kept} == {"continues"} and "[1, 2]" in {r[1] for r in kept}


def test_at_most_so_many_questions_a_run(conn):
    pages = scanned(["a", "q", "c", "z"])
    judge = Judge()
    assert continues.judge(pages, judge, Answers(conn), most=2) == 2
    assert continues.judge(scanned(["a", "q", "c", "z"]), judge, Answers(conn), most=2) == 2
    assert len(judge.asked) == 4  # the rest the next time


def test_a_judge_that_fails_is_left_alone_and_the_rules_sort(conn):
    pages = scanned(["a", "q", "c", "z"])
    judge = Judge(fail=ProviderError("Couldn't reach Lindley's own AI"))
    assert continues.judge(pages, judge, Answers(conn)) == 1  # asked once, then no more
    assert all(not p.runs_into for p in pages)
    assert conn.execute("SELECT COUNT(*) FROM ai_answers").fetchone()[0] == 0  # asked again


def test_sorting_asks_the_judge_first(conn):
    load(conn, [TruePage(t, "memoir", i, "page") for i, t in enumerate(TYPESCRIPT[:3])])
    judge = Judge()
    report = assemble(conn, judge=judge)
    assert report.judged == len(judge.asked)


def test_the_judge_runs_on_its_own_and_its_calls_dont_count_against_limits(conn, settings):
    settings.ai.providers["own"] = ProviderConfig(type="fake", daily_limit=1, allow="auto")
    settings.ai.jobs["continues"] = JobConfig(connection="own")
    load(
        conn,
        [TruePage(t, "memoir", i, "page") for i, t in enumerate(TYPESCRIPT[:3])],
        prefix="part",
    )
    assert judge_for(settings) is not None
    report = sort_with(conn, settings, None, True)
    calls = conn.execute("SELECT provider, purpose, automatic FROM ai_calls").fetchall()
    assert len(calls) == report.judged
    assert {tuple(c) for c in calls} <= {("own", "continues", 1)}
    assert allowance.calls_today(conn, "own", worked=True) == 0


def test_only_an_ai_on_your_own_computers_checks_pages(settings):
    settings.ai.providers["claude"] = ProviderConfig(type="anthropic")
    settings.ai.jobs["continues"] = JobConfig(connection="claude")
    assert judge_for(settings) is None
    settings.ai.providers["own"] = ProviderConfig(type="builtin")
    settings.ai.jobs["continues"] = JobConfig(connection="own")
    assert judge_for(settings) is None  # its model isn't downloaded


def test_the_benches_keep_answers_across_their_scratch_databases(tmp_path):
    from lindley.assembler.bench import kept_answers

    kept = tmp_path / "kept.db"
    judge = Judge()
    for run in ("one", "two"):
        db = tmp_path / f"{run}.db"
        init_db(db)
        c = connect(db)
        with kept_answers(c, kept):
            continues.judge(scanned(["a", "q", "c", "z"]), judge, Answers(c), None)
        c.close()
    assert judge.asked and len(judge.asked) == len(set(judge.asked))  # each pair asked once
