# Benching Lindley's AI

How to measure Lindley's own AI (llama.cpp's `llama-server`, `backend/src/lindley/localai/`) and the rules it helps, on real scans, and what was learned doing it. The results are in [database.md](database.md): "Local models" (on Ollama, before Lindley had its own AI) and "Lindley's own AI, measured" (October 2026).

Test with Lindley's own engine and models, not Ollama. Ollama and LM Studio stay supported through the `local` connection, but they aren't what Lindley ships.

## Setup

Every script reads the settings Lindley uses (`settings.json` at the repo root, for the dev library). Run them from the repo root with the backend's Python, `backend\.venv\Scripts\python`.

- **Models.** They go in `ai.local.models_dir`, which in the dev settings is `data/models` (gitignored):
  - `engine/b11429/` holds `llama-server.exe` and its DLLs.
  - Each model is in a folder named by its id.
  - `logs/` keeps each server's log and preset, the last 10 (`server-<time>-<pid>.log` and `.ini`).
- **Getting models:**
  - `python backend/scripts/local_ai.py list` shows what's downloaded.
  - `python backend/scripts/local_ai.py download gemma-4-e4b` downloads a model, and the engine if it's missing. Files are checked by SHA-256, and a cut-off download goes on from where it stopped.
- **Devices.** `python backend/scripts/local_ai.py devices` lists what llama.cpp can use. Every bench takes `--device`:
  - `none`: the processor alone. Use it for laptop-like timings.
  - `Vulkan0`: on the dev PC, the RTX 2080 SUPER (8 GB).
  - `Vulkan1`: on the dev PC, the Intel UHD 630. Its 2021 driver can't be opened (`ErrorDeviceLost`), so it can't be benched until the driver is updated.
  - Left out, llama.cpp uses any graphics it can. If they fail to load a model, Lindley falls back to the processor alone.
- **Serving by hand.** `python backend/scripts/local_ai.py serve gemma-4-e4b --device none` runs a server with the model loaded and prints its address. When stopped, it prints the most memory it took.

## The benches

Each starts its own server, with the settings' models folder, and stops it at the end. The most memory each model took is its processes' peak working set, the weights read from disk included. It's only meaningful on the processor: on a graphics card, the driver's memory counts too.

| Script | Measures | Typical run |
|---|---|---|
| `bench_continues.py --real data/lindley.db --models gemma-4-e2b --lines 4 --negatives 150 --device none --out data/bench/continues_cpu.json` | Whether page B carries on from A (`p_yes`): AUC over 424 pairs, and where `runs_on` fires (the rules' blind spot). It writes each model's scores after that model | 12–35 minutes a model on the processor. `--negatives 20` (164 pairs) is enough for a timing |
| `bench_reading.py --db data/lindley.db --pages 69,71,75,77,78,97,166,172,258,282 --limit 10 --reference anthropic --models gemma-4-e4b --device none --out data/bench/reading` | Reading hard pages: CER against Claude's reading, seconds a page | About 30 minutes on the processor, 1 on the graphics card |
| `bench_assembler.py --real data/lindley.db --orders in_order --habits one_folder --seeds 1` | Sorting the 23 documents with the rules alone. Add `--ai gemma-4-e4b` to sort with a model, or `--judge gemma-4-e2b --answers data/bench/judge-e2b.db` for the continuation check | 15 s for the rules alone; 3–20 minutes with a model on the graphics card. It prints the seconds each sorting call took; `--max-calls 3` stops asking after three, for a timing on the processor (about 6 minutes) |
| `fit_assembler.py --real data/lindley.db --answers data/bench/judge-qwen.db --only lm_continues` | A weight fitted to the answer key | 3 minutes |
| `bench_meaning.py --real data/lindley.db` | What pages are about: how often a page's most alike page is from its own document (EmbeddingGemma 122 of 147, rare words 117) | 10 s on the graphics card |

The answer key is the dev library's hand-made PDFs, matched back to their scans (`assembler.bench.real_answers`). Never ask the user to transcribe pages: Claude's reading stands in for the right answer.

## Keep these

They're in `data/bench/`, which is gitignored:

- **`reading/page-N.anthropic.txt`**: Claude's readings of the ten hardest pages. They were paid for, and a bench given the same `--out` (or a copy of them) uses them again rather than asking Claude. Copy them into a new `--out` folder before trying a variation.
- **`judge-<model>.db`**: each model's continuation answers, by the text asked about. Pass them with `--answers`, and a pair is never asked twice. A weight experiment then needs no model at all:

  ```
  python -c "import sys, runpy; from lindley.assembler.weights import WEIGHTS; WEIGHTS['lm_continues'] = 0.3; sys.argv = ['b', '--real', 'data/lindley.db', '--orders', 'in_order', '--habits', 'one_folder', '--seeds', '1', '--answers', 'data/bench/judge-qwen.db']; runpy.run_path('backend/scripts/bench_assembler.py', run_name='__main__')"
  ```
- **`*.txt` and `*.json`**: what each run printed, and the scores for each pair.

## Things to know

- **Run one bench at a time,** and send its output to a file (`> data/bench/x.txt 2>&1`) rather than through `| tail`, which loses the results when a run ends badly. Anything else on the processor (tests, a second bench) changes the timings.
- **The "tesseract" row in `bench_reading.py`** is each page's current reading. For the ten hardest pages that's now Claude's (from the October 2026 test), so it scores near 0 against Claude.
- **The `bench_assembler.py` "proposed" line** is the groups the rules propose before the confidence bars. With `--judge` or `--answers`, it's worked out with the model's answers in hand.
- **llama-server out of the box doesn't suit a laptop.** Lindley sets these in the preset (`localai/server.py`):
  - `cache-ram = 0`. Otherwise up to 8 GB of earlier prompts is kept in memory: Qwen3.5 4B took 9.8 GB in place of 4.4.
  - `parallel = 1`: all the context for one request.
  - For an embedding model, `batch-size` and `ubatch-size` as large as its context. Otherwise a page over 512 tokens is refused.
- **Keep-alive.** llama-server closes a kept-open connection after a while, and a call sent on it just then is lost ("Server disconnected without sending a response"), about one in 400. That looked like the server dying. The `builtin` connector makes a new connection for each call. `bench_continues.py` also asks once more after a lost call.
- **Images.** Gemma 4 takes each page as about 1,100 image tokens, and on a processor reading them is most of the time: about 95 s of E4B's 165 s a page. Fewer tokens (the `LLAMA_ARG_IMAGE_MAX_TOKENS` environment variable, which the server passes to its models) read much worse: 560 tokens gave CER 0.29, 280 gave 0.41, against 0.18. So Lindley keeps the model's own.
- **Loading.** On the processor, Gemma 4 E4B takes about 55 s to load and E2B 10 s. On the graphics card E4B takes about 30 s the first time, while Vulkan builds its shaders.
- **Thinking** is off for every quick call (`chat_template_kwargs.enable_thinking: false`), and on for Ask Lindley's streamed answers. llama.cpp has no per-request `reasoning_effort`.
- **Seeing what a server did.** Read its log in `data/models/logs/`. The router's lines have no prefix; each model's process prefixes its own with `[port]`. A model that failed to load shows `"failed": true` in `GET /models`, with its exit code.

## Still to bench

- Laptops: a recent one with 8 GB and with 16 GB, and an old one. Middle and High were set from a 2019 desktop processor (i5-9400, DDR4).
- Built-in graphics with a current driver (Intel Iris Xe, AMD Radeon 780M), which the Vulkan build should use at about twice the processor's speed.
- Whether Middle should read handwriting itself: E4B takes nearly 3 minutes a hard page on the i5-9400.
- Gemma 4 26B-A4B for High with the memory for it. It sorted the 23 documents far better than E4B (22 documents made, 1 wrong, against 8 made, 2 wrong) and needs about 16 GB. Not decided yet.
- A local model for Ask Lindley's answers, judged against Claude's.
