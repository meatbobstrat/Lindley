"""AI connectors: one file each. Lindley finds every module here when it starts.

To add one, drop a module in this folder that defines:

- `INFO`: a `ConnectorInfo` (its id, label, where it runs, which jobs it can do, default models).
- `Provider`: a class built as `Provider(config=..., model=..., api_key=...)`, implementing
  whichever of `chat`/`chat_stream`, `transcribe` and `embed` its jobs need, plus `check()`,
  a cheap call that returns a short message for "Test connection".

Call the AI through its company's own library (SDK), the way its documentation shows, and
turn the library's errors into a ProviderError a person can read (`_common.py`). The library
keeps up with the AI's API. Turn its own retries off: the throttle tries again, only when the
AI says it's busy.

Modules starting with `_` are shared helpers, not connectors.
"""
