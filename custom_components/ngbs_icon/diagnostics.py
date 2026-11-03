"""Diagnostics for the NGBS iCON integration.

The download holds everything the controller reports, read-only: the decoded
poll, the JSON-only settings, the raw JSON ``RELOAD`` response and every
non-zero documented Modbus register.
"""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from .coordinator import IconConfigEntry

REDACTED = "**REDACTED**"
SENSITIVE_PARTS = (
    "sysid",
    "key",
    "pass",
    "pwd",
    "pin",
    "token",
    "user",
    "email",
    "mac",
    "ssid",
)


def _redact(value: Any) -> Any:
    """Return ``value`` with credentials and network identifiers masked."""
    if isinstance(value, dict):
        return {
            key: REDACTED
            if any(part in str(key).lower() for part in SENSITIVE_PARTS)
            else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: IconConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data
    return {
        "data": coordinator.data,
        "settings": coordinator.settings,
        "raw": _redact(await coordinator.async_read_raw()),
    }
