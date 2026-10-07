"""What Lindley's own AI is made of: one llama.cpp build, and the models it can download.

Every file is pinned: a fixed build and a fixed revision of each model, with its size and
SHA-256, so what's downloaded is what was benched, and a file changed on the way is refused.
Updating one means benching it again (scripts/bench_continues.py, bench_reading.py) and putting
its new size and checksum here: GitHub's release API gives the build's (`digest`), and Hugging
Face's tree API the models' (`lfs.oid`).

Everything goes in one folder (ai.local.models_dir): the engine in `engine/<build>/`, and each
model in a folder named by its id.
"""

from __future__ import annotations

import platform
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from lindley.providers.base import Job


@dataclass(frozen=True)
class File:
    name: str
    url: str
    size: int  # bytes
    sha256: str


def _hf(repo: str, revision: str, name: str, size: int, sha256: str) -> File:
    return File(name, f"https://huggingface.co/{repo}/resolve/{revision}/{name}", size, sha256)


@dataclass(frozen=True)
class Engine:
    build: str  # llama.cpp's build number, e.g. "b11429"
    file: File  # the zip, or the .tar.gz
    program: str  # the server, in it

    def folder(self, root: Path) -> Path:
        return root / "engine" / self.build

    def path(self, root: Path) -> Path:
        return self.folder(root) / self.program

    def installed(self, root: Path) -> bool:
        return self.path(root).is_file()


@dataclass(frozen=True)
class Model:
    id: str  # the model's name in settings (ai.jobs) and to the server
    label: str
    files: tuple[File, ...]  # the model, then its image projector if it reads images
    jobs: frozenset[Job]
    context: int  # tokens the server gives each request, question and answer
    memory_gb: float  # about how much memory it takes in use
    # More of llama-server's settings for this model, as in its --models-preset file
    preset: Mapping[str, str] = field(default_factory=dict)

    def folder(self, root: Path) -> Path:
        return root / self.id

    def installed(self, root: Path) -> bool:
        return all((self.folder(root) / f.name).is_file() for f in self.files)

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)


# llama.cpp v0.6.0 (build b11429, 5 October 2026). On Windows and Ubuntu the Vulkan build: it
# uses built-in graphics and graphics cards of any make, and the processor alone when there are
# none. On a Mac, llama.cpp's own build, which uses Metal.
_RELEASE = "https://github.com/ggml-org/llama.cpp/releases/download/b11429/"


def _build(name: str, size: int, sha256: str, program: str = "llama-server") -> Engine:
    return Engine("b11429", File(name, _RELEASE + name, size, sha256), program)


# By system and processor (this_computer())
ENGINES = {
    "win32-x64": _build(
        "llama-b11429-bin-win-vulkan-x64.zip",
        33337769,
        "1bfe78ad9168b79fa02bf67f6af9f5e17a966d824d77238517f7bef12ac73b36",
        "llama-server.exe",
    ),
    "linux-x64": _build(
        "llama-b11429-bin-ubuntu-vulkan-x64.tar.gz",
        31636673,
        "632c4e98feba2b94407a2130e3133e0c3aefb0ea1ab41337e926d8bfafdd0b74",
    ),
    "linux-arm64": _build(
        "llama-b11429-bin-ubuntu-vulkan-arm64.tar.gz",
        24845090,
        "702d99c4219b4314cc6d3b10fb41ae96cbfc68226305c2681269dfb51487af34",
    ),
    "darwin-arm64": _build(
        "llama-b11429-bin-macos-arm64.tar.gz",
        11971406,
        "740288ec6887be94280a5dfa25b5e23a78285cab104519e6c7e218904ee82459",
    ),
    "darwin-x64": _build(
        "llama-b11429-bin-macos-x64.tar.gz",
        11487431,
        "29ac3ea02be6bd143e824973f2cc5fa74bc4094393a9eaab0ff6814f19dd8522",
    ),
}


def this_computer() -> str:
    """Its system and processor, as ENGINES names them: "win32-x64", "darwin-arm64"..."""
    machine = platform.machine().lower()
    arch = {"amd64": "x64", "x86_64": "x64", "aarch64": "arm64"}.get(machine, machine)
    return f"{sys.platform}-{arch}"


def engine() -> Engine | None:
    """The build for this computer. None: there isn't one (Windows on Arm, for one)."""
    return ENGINES.get(this_computer())


_READS = frozenset({"vision", "assemble", "chat", "continues"})

MODELS: dict[str, Model] = {
    m.id: m
    for m in (
        # Google's own Q4_0 conversions, from llama.cpp's makers (ggml-org)
        Model(
            id="gemma-4-e4b",
            label="Gemma 4 E4B",
            files=(
                _hf(
                    "ggml-org/gemma-4-E4B-it-GGUF",
                    "b8093469224f83f5c38f691eb906c380e9e63114",
                    "gemma-4-E4B-it-Q4_0.gguf",
                    4590807392,
                    "a555b900214b477d8880e7832e0b8925e139b0159640036b09fe472b6f2097f2",
                ),
                _hf(
                    "ggml-org/gemma-4-E4B-it-GGUF",
                    "b8093469224f83f5c38f691eb906c380e9e63114",
                    "mmproj-gemma-4-E4B-it-Q8_0.gguf",
                    559874816,
                    "197f49a93027f9843772bd24a6a9e0be2a32a788de5a3def330e9c585d86edd1",
                ),
            ),
            jobs=_READS,
            context=16384,
            memory_gb=6.5,
        ),
        Model(
            id="gemma-4-e2b",
            label="Gemma 4 E2B",
            files=(
                _hf(
                    "ggml-org/gemma-4-E2B-it-GGUF",
                    "b4243c156154b6dca9324415f8c7ccc098b4aed1",
                    "gemma-4-E2B-it-Q4_0.gguf",
                    2841481184,
                    "8e30dff3ac4c8434c49a7036fa15564bdbb6044e42bf04550bf1a096ad7e6a52",
                ),
            ),
            jobs=frozenset({"assemble", "chat", "continues"}),
            context=8192,
            memory_gb=4,
        ),
        Model(
            id="gemma-4-26b-a4b",
            label="Gemma 4 26B",
            files=(
                _hf(
                    "ggml-org/gemma-4-26B-A4B-it-GGUF",
                    "bb4531cda34d1ea09d9814959ed4d5833cf2a4c8",
                    "gemma-4-26B-A4B-it-Q4_0.gguf",
                    14618145824,
                    "d208665ab1cd3a69f7a9a4bc59430e8448c8093d9b06334f566ac59d6d504a03",
                ),
                _hf(
                    "ggml-org/gemma-4-26B-A4B-it-GGUF",
                    "bb4531cda34d1ea09d9814959ed4d5833cf2a4c8",
                    "mmproj-gemma-4-26B-A4B-it-Q8_0.gguf",
                    806408320,
                    "cc4e855736da450bf1e162d8cccfe0ad685727d0c9e04ef7dd8d884f3121039b",
                ),
            ),
            jobs=_READS,
            context=16384,
            memory_gb=18,
        ),
        # Unsloth's: there's no ggml-org conversion of Qwen3.5
        Model(
            id="qwen3.5-4b",
            label="Qwen3.5 4B",
            files=(
                _hf(
                    "unsloth/Qwen3.5-4B-GGUF",
                    "e87f176479d0855a907a41277aca2f8ee7a09523",
                    "Qwen3.5-4B-Q4_K_M.gguf",
                    2740937888,
                    "00fe7986ff5f6b463e62455821146049db6f9313603938a70800d1fb69ef11a4",
                ),
            ),
            jobs=frozenset({"assemble", "chat", "continues"}),
            context=8192,
            memory_gb=3.5,
        ),
        Model(
            id="embeddinggemma",
            label="EmbeddingGemma",
            files=(
                _hf(
                    "ggml-org/embeddinggemma-300M-GGUF",
                    "0f741b5a6585bd53aeb15cd1372c56f2a0f65e12",
                    "embeddinggemma-300M-Q8_0.gguf",
                    333590944,
                    "b5ce9d77a3fc4b3b39ccb5643c36777911cc4eb46a66962eadfa3f5f60490d63",
                ),
            ),
            jobs=frozenset({"embed"}),
            context=2048,
            memory_gb=0.6,
            # A text is embedded in one batch, which out of the box holds 512 tokens: a page
            # can be longer
            preset={"embeddings": "true", "batch-size": "2048", "ubatch-size": "2048"},
        ),
    )
}
