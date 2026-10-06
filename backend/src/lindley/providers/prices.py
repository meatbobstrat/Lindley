"""What a call costs, estimated from the tokens it used and the price list below.

The bill from the AI's company is the real figure: prices change, and a call the AI ran again on
another model (Anthropic's fallbacks) is priced here as the model it was sent to. Models not
listed, and AIs on your own computers, have no cost (None). A dated snapshot of a listed model
("gpt-6.1-sol-2026-09-15", "claude-haiku-4-5-20251001") is priced as that model.

List prices for the standard tier, checked October 2026. Update them when they change:
- Anthropic: https://platform.claude.com/docs/en/about-claude/pricing
- OpenAI: https://developers.openai.com/api/docs/pricing
- Google: https://ai.google.dev/gemini-api/docs/pricing
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import date

from lindley.providers.base import Usage


@dataclass(frozen=True)
class Price:
    """US dollars a million tokens. A prompt longer than `long_over` tokens (cache included) is
    priced at `long` for the whole call, as OpenAI and Google charge."""

    sent: float
    written: float  # thinking included
    cached: float  # read from the cache
    cache_write: float  # written to the cache
    long_over: int | None = None
    long: Price | None = None


def _claude(sent: float, written: float, cached: float) -> Price:
    return Price(sent, written, cached, sent * 1.25)  # the 5-minute cache, Anthropic's default


def _openai(row: tuple, long: tuple | None = None) -> Price:
    """A row of OpenAI's table: input, cached input, cache writes, output. None for "-": no
    discount for a cached read, and a write costs as input."""

    def price(sent, cached, cache_write, written) -> Price:
        return Price(sent, written, sent if cached is None else cached, cache_write or sent)

    if long is None:
        return price(*row)
    return replace(price(*row), long_over=272_000, long=price(*long))


def _gemini(sent: float, written: float, cached: float, long: tuple | None = None) -> Price:
    """Gemini's output price counts thinking. It doesn't charge for writing to its cache, only
    for keeping one by the hour, which Lindley never asks for."""
    if long is None:
        return Price(sent, written, cached, sent)
    return Price(sent, written, cached, sent, 200_000, Price(*long, long[0]))


# Each model's prices, newest last: (from, price). A price with no date has no start.
PRICES: dict[str, list[tuple[date | None, Price]]] = {
    "claude-fable-5-1": [(None, _claude(10.0, 50.0, 0.25))],
    "claude-fable-5": [(None, _claude(10.0, 50.0, 1.0))],
    "claude-opus-5-5": [(None, _claude(4.0, 20.0, 0.20))],
    "claude-opus-5": [(None, _claude(5.0, 25.0, 0.50))],
    "claude-opus-4-8": [(None, _claude(5.0, 25.0, 0.50))],
    "claude-opus-4-7": [(None, _claude(5.0, 25.0, 0.50))],
    "claude-opus-4-6": [(None, _claude(5.0, 25.0, 0.50))],
    "claude-opus-4-5": [(None, _claude(5.0, 25.0, 0.50))],
    "claude-sonnet-5-5": [(None, _claude(2.0, 10.0, 0.20))],
    "claude-sonnet-5": [(None, _claude(2.0, 10.0, 0.20))],
    "claude-sonnet-4-6": [(None, _claude(3.0, 15.0, 0.30))],
    "claude-sonnet-4-5": [(None, _claude(3.0, 15.0, 0.30))],
    "claude-haiku-4-5": [(None, _claude(1.0, 5.0, 0.10))],
    "gpt-6-astra": [(None, _openai((10.0, 1.0, 12.5, 50.0), (20.0, 2.0, 25.0, 75.0)))],
    "gpt-6.1-sol": [(None, _openai((2.0, 0.10, 2.5, 10.0), (4.0, 0.20, 5.0, 15.0)))],
    "gpt-6-sol": [(None, _openai((2.0, 0.20, 2.5, 10.0), (4.0, 0.40, 5.0, 15.0)))],
    "gpt-6-luna": [(None, _openai((0.10, 0.01, 0.125, 0.50), (0.20, 0.02, 0.25, 0.75)))],
    "gpt-5.6-sol": [(None, _openai((4.0, 0.40, 5.0, 20.0), (8.0, 0.80, 10.0, 30.0)))],
    "gpt-5.6-terra": [(None, _openai((2.0, 0.20, 2.5, 12.0), (4.0, 0.40, 5.0, 18.0)))],
    "gpt-5.6-luna": [(None, _openai((0.20, 0.02, 0.25, 1.20), (0.40, 0.04, 0.50, 1.80)))],
    "gpt-5.5": [(None, _openai((5.0, 0.50, None, 30.0), (10.0, 1.0, None, 45.0)))],
    "gpt-5.5-pro": [(None, _openai((30.0, None, None, 180.0), (60.0, None, None, 270.0)))],
    "gpt-5.4": [(None, _openai((2.5, 0.25, None, 15.0), (5.0, 0.50, None, 22.5)))],
    "gpt-5.4-mini": [(None, _openai((0.75, 0.075, None, 4.50)))],
    "gpt-5.4-nano": [(None, _openai((0.20, 0.02, None, 1.25)))],
    "gpt-5.4-pro": [(None, _openai((30.0, None, None, 180.0), (60.0, None, None, 270.0)))],
    "gpt-5.2": [(None, _openai((1.75, 0.175, None, 14.0)))],
    "gpt-5.2-pro": [(None, _openai((21.0, None, None, 168.0)))],
    "gpt-5.1": [(None, _openai((1.25, 0.125, None, 10.0)))],
    "gpt-5": [(None, _openai((1.25, 0.125, None, 10.0)))],
    "gpt-5-mini": [(None, _openai((0.25, 0.025, None, 2.0)))],
    "gpt-5-nano": [(None, _openai((0.05, 0.005, None, 0.40)))],
    "gpt-5-pro": [(None, _openai((15.0, None, None, 120.0)))],
    "text-embedding-3-small": [(None, _openai((0.02, None, None, 0.0)))],
    "text-embedding-3-large": [(None, _openai((0.13, None, None, 0.0)))],
    # Gemini 3.6–3.8 Flash cost half until the end of 2026
    "gemini-3.8-flash": [
        (None, _gemini(0.75, 3.75, 0.075)),
        (date(2027, 1, 1), _gemini(1.50, 7.50, 0.15)),
    ],
    "gemini-3.7-flash": [
        (None, _gemini(0.75, 3.75, 0.075)),
        (date(2027, 1, 1), _gemini(1.50, 7.50, 0.15)),
    ],
    "gemini-3.6-flash": [
        (None, _gemini(0.75, 3.75, 0.075)),
        (date(2027, 1, 1), _gemini(1.50, 7.50, 0.15)),
    ],
    "gemini-3.5-flash": [(None, _gemini(1.50, 9.0, 0.15))],
    "gemini-3.5-flash-lite": [(None, _gemini(0.30, 2.50, 0.03))],
    "gemini-3.1-flash-lite": [(None, _gemini(0.25, 1.50, 0.025))],
    "gemini-3.1-pro-preview": [(None, _gemini(2.0, 12.0, 0.20, (4.0, 18.0, 0.40)))],
    "gemini-2.5-pro": [(None, _gemini(1.25, 10.0, 0.125, (2.50, 15.0, 0.25)))],
    "gemini-2.5-flash": [(None, _gemini(0.30, 2.50, 0.03))],
    "gemini-2.5-flash-lite": [(None, _gemini(0.10, 0.40, 0.01))],
    "gemini-embedding-2": [(None, _gemini(0.20, 0.0, 0.20))],
}

# What a dated snapshot adds to a model's id: -20251001, -2026-09-15, -001
_SNAPSHOT = re.compile(r"-(\d{8}|\d{4}-\d{2}-\d{2}|\d{3})")


def price_of(model: str | None, on: date | None = None) -> Price | None:
    """The model's price on that day (today if not given); None when it isn't listed."""
    if not model:
        return None
    prices = PRICES.get(model)
    if prices is None:
        listed = [m for m in PRICES if model.startswith(m) and _SNAPSHOT.fullmatch(model[len(m) :])]
        if not listed:
            return None
        prices = PRICES[max(listed, key=len)]
    on = on or date.today()
    return [p for since, p in prices if since is None or since <= on][-1]


def cost(usage: Usage, on: date | None = None) -> float | None:
    """The call's cost in US dollars, at the prices of that day (today if not given); None
    when the model's price isn't known."""
    price = price_of(usage.model, on)
    if price is None:
        return None
    prompt = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens
    if price.long is not None and price.long_over is not None and prompt > price.long_over:
        price = price.long
    return (
        usage.input_tokens * price.sent
        + usage.output_tokens * price.written
        + usage.cache_read_tokens * price.cached
        + usage.cache_write_tokens * price.cache_write
    ) / 1_000_000
