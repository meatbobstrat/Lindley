"""API keys, kept in the system's credential store: Windows Credential Manager, or the macOS
Keychain. Never in settings.json or the library, so both are safe to back up and copy.

Each key is stored under the service "Lindley", with its connection's id as the user name.
"""

from __future__ import annotations

import logging

import keyring
from keyring.errors import KeyringError, PasswordDeleteError

from lindley.providers.base import ProviderError

log = logging.getLogger(__name__)

SERVICE = "Lindley"


def get_key(name: str) -> str | None:
    try:
        return keyring.get_password(SERVICE, name)
    except KeyringError:
        log.exception("Couldn't read the key for %r from the credential store", name)
        return None


def set_key(name: str, key: str) -> None:
    try:
        keyring.set_password(SERVICE, name, key)
    except KeyringError as e:
        raise ProviderError(f"Couldn't save the key in the credential store: {e}") from e


def delete_key(name: str) -> None:
    try:
        keyring.delete_password(SERVICE, name)
    except PasswordDeleteError:
        pass  # there was none
    except KeyringError:
        log.exception("Couldn't delete the key for %r from the credential store", name)


def key_hint(name: str) -> str | None:
    """The key's last 4 characters, so a person can tell which key is saved."""
    key = get_key(name)
    return key[-4:] if key else None
