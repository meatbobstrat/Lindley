"""API keys, kept in the system's credential store: Windows Credential Manager, the macOS
Keychain, or on Linux the desktop's keyring (GNOME Keyring or KWallet). Never in settings.json
or the library, so both are safe to back up and copy.

Each key is stored under the service "Lindley", with its connection's id as the user name.
"""

from __future__ import annotations

import logging

import keyring
from keyring.errors import KeyringError, NoKeyringError, PasswordDeleteError

from lindley.providers.base import ProviderError

log = logging.getLogger(__name__)

SERVICE = "Lindley"
# A Linux desktop with no keyring running: Lindley won't keep a key in a plain file instead
NO_KEYRING = (
    "This computer has no keyring to keep the key in safely. On Ubuntu, GNOME Keyring comes "
    "with the desktop (sudo apt install gnome-keyring if it was removed). Or keep the key in "
    "an environment variable, and name it in the connection's api_key_env in settings.json."
)


def get_key(name: str) -> str | None:
    try:
        return keyring.get_password(SERVICE, name)
    except NoKeyringError:
        return None  # nowhere a key could have been saved
    except KeyringError:
        log.exception("Couldn't read the key for %r from the credential store", name)
        return None


def set_key(name: str, key: str) -> None:
    try:
        keyring.set_password(SERVICE, name, key)
    except NoKeyringError as e:
        raise ProviderError(NO_KEYRING) from e
    except KeyringError as e:
        raise ProviderError(f"Couldn't save the key in the credential store: {e}") from e


def delete_key(name: str) -> None:
    try:
        keyring.delete_password(SERVICE, name)
    except (PasswordDeleteError, NoKeyringError):
        pass  # there was none
    except KeyringError:
        log.exception("Couldn't delete the key for %r from the credential store", name)


def key_hint(name: str) -> str | None:
    """The key's last 4 characters, so a person can tell which key is saved."""
    key = get_key(name)
    return key[-4:] if key else None
