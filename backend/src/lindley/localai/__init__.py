"""Lindley's own AI: llama.cpp's llama-server, with models Lindley downloads when a person chooses
them. `catalog` says what there is, `download` fetches it, and `server` runs it.

Nothing here touches the network on its own: files are downloaded only when a person asks, and
the server runs offline (design/database.md, "Local models on a CPU").
"""
