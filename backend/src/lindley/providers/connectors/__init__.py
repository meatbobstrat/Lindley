"""AI connectors: one file each. Lindley finds every module here when it starts.

To add one, drop a module in this folder that defines:

- `INFO`: a `ConnectorInfo` (its id, label, where it runs, which jobs it can do, default models).
- `Provider`: a class built as `Provider(config=..., model=..., api_key=...)`, implementing
  whichever of `chat`/`chat_stream`, `transcribe` and `embed` its jobs need, plus `check()`,
  a cheap call that returns a short message for "Test connection".

Modules starting with `_` are shared helpers, not connectors.
"""
