"""What a call costs, estimated from the tokens it used and the price list below.

The bill from the AI's company is the real figure: prices change, and a call the AI ran again on
another model (Anthropic's fallbacks) is priced here as the model it was sent to. Models not
listed, and AIs on your own computers, have no cost (None).
"""

from __future__ import annotations

from lindley.providers.base import Usage

# US dollars a million tokens: (input, output, read from the cache). Writing to the cache costs
# 1.25 times input (Anthropic's 5-minute cache).
PER_MILLION: dict[str, tuple[float, float, float]] = {
    "claude-fable-5-1": (10.0, 50.0, 0.25),
    "claude-opus-5-5": (4.0, 20.0, 0.20),
    "claude-opus-5": (5.0, 25.0, 0.50),
    "claude-sonnet-5-5": (2.0, 10.0, 0.20),
    "claude-haiku-4-5": (1.0, 5.0, 0.10),
}
CACHE_WRITE = 1.25


def cost(usage: Usage) -> float | None:
    """The call's cost in US dollars; None when the model's price isn't known."""
    price = PER_MILLION.get(usage.model or "")
    if price is None:
        return None
    sent, written, cached = price
    return (
        usage.input_tokens * sent
        + usage.output_tokens * written
        + usage.cache_read_tokens * cached
        + usage.cache_write_tokens * sent * CACHE_WRITE
    ) / 1_000_000
