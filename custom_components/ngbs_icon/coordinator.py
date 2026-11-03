"""Data update coordinator for the NGBS iCON integration."""

from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ID, CONF_IP_ADDRESS, CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    CONF_INVENTORY,
    DEFAULT_MODBUS_PORT,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)
from .modbus_client import IconModbusClient, IconModbusError
from .names import IconJsonClient, IconJsonError, extract_settings

_LOGGER = logging.getLogger(__name__)

type IconConfigEntry = ConfigEntry[IconDataUpdateCoordinator]

# The controller's internal regulation cycle takes ~1-1.2s to fold a write
# into its read-only mirror registers (measured live); wait this long before
# reading back so the post-write refresh doesn't just observe the stale value.
# Cross-unit BMS propagation (e.g. a relay on one icon driven by a thermostat
# on another) takes longer to settle the more icon units are chained on the
# bus, so each slave adds a bit more on top of the single-unit baseline.
WRITE_SETTLE_BASE_DELAY = 1.5
WRITE_SETTLE_PER_SLAVE_DELAY = 0.5

# Settings such as the hysteresis exist only on the JSON port, where the
# controller serves one client at a time and may be busy with the NGBS app. They
# rarely change, so they are read at most hourly and a failure never fails a poll.
SETTINGS_REFRESH_SECONDS = 3600
SETTINGS_RETRY_SECONDS = 300
JSON_TIMEOUT = 5.0


class IconDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Poll the controller, reconnecting over Modbus for each poll."""

    config_entry: IconConfigEntry

    def __init__(self, hass: HomeAssistant, entry: IconConfigEntry) -> None:
        """Initialize the coordinator and the Modbus client."""
        self.sysid: str = entry.data[CONF_ID]
        self.inventory: dict[str, Any] = entry.data.get(CONF_INVENTORY, {})
        # Device registry ids of the iCON controllers, keyed by icon key; filled
        # in at setup so thermostat devices can reference their controller.
        self.icon_device_ids: dict[str, str] = {}
        host = entry.data[CONF_IP_ADDRESS]
        self.client = IconModbusClient(host, port=DEFAULT_MODBUS_PORT)
        self.json_client: IconJsonClient | None = (
            IconJsonClient(host, self.sysid, timeout=JSON_TIMEOUT)
            if self.sysid
            else None
        )
        self.settings: dict[str, Any] = {}
        self._settings_due: float = 0.0
        scan_interval = entry.options.get(
            CONF_SCAN_INTERVAL,
            entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
        )
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=timedelta(seconds=scan_interval),
        )

    async def _async_fetch_and_cache(self) -> dict[str, Any]:
        """Fetch the full dataset, caching discovered device indices in-memory.

        Config flow setup/reconfigure already saves ``device_indices`` into
        the persisted inventory so ``async_get_data`` can skip probing every
        device slot on every poll. Entries created before that (or that
        haven't been reconfigured since) start without a cached list, which
        would otherwise mean a full probe on *every* poll rather than just
        setup/init. Caching it here after the first successful read - however
        it was obtained - fixes that for the rest of this run without forcing
        a reconfigure; it isn't persisted to storage, so a fresh probe still
        happens once after each Home Assistant restart until Reconfigure is run.
        """
        data = await self.client.async_get_data(self.inventory)
        if not self.inventory.get("device_indices"):
            self.inventory["device_indices"] = sorted(
                int(icon_key) - 1 for icon_key in data["icons"]
            )
        await self._async_refresh_settings()
        self._apply_settings(data)
        return data

    async def _async_refresh_settings(self) -> None:
        """Re-read the JSON-only settings when due. Best effort."""
        now = time.monotonic()
        if self.json_client is None or now < self._settings_due:
            return
        try:
            raw = await self.json_client.async_get_settings()
        except IconJsonError as err:
            _LOGGER.debug("Reading iCON settings over JSON failed: %s", err)
            self._settings_due = now + SETTINGS_RETRY_SECONDS
            return
        self.settings = extract_settings(raw)
        self._settings_due = now + SETTINGS_REFRESH_SECONDS

    def _apply_settings(self, data: dict[str, Any]) -> None:
        """Merge the JSON-only settings and the off point into a poll."""
        hysteresis = self.settings.get("hysteresis")
        data["system"]["hysteresis"] = hysteresis
        data["system"]["uptime_hours"] = self.settings.get("uptime_hours")
        per_thermostat = self.settings.get("thermostats", {})
        for icon in data["icons"].values():
            for thermostat_id, thermostat in icon["thermostats"].items():
                thermostat.update(per_thermostat.get(thermostat_id, {}))
                target = thermostat["target"]
                thermostat["demand_off_temp"] = (
                    None
                    if target is None or hysteresis is None
                    else round(
                        target - hysteresis
                        if thermostat["cooling"]
                        else target + hysteresis,
                        2,
                    )
                )

    async def async_read_raw(self) -> dict[str, Any]:
        """Return the raw JSON and Modbus data for the diagnostics download."""
        result: dict[str, Any] = {"json": None, "modbus": None}
        if self.json_client is not None:
            try:
                result["json"] = await self.json_client.async_get_settings()
            except IconJsonError as err:
                result["json_error"] = str(err)
        device_indices = self.inventory.get("device_indices") or [
            int(icon_key) - 1 for icon_key in self.data["icons"]
        ]
        try:
            result["modbus"] = await self.client.async_read_raw(device_indices)
        except IconModbusError as err:
            result["modbus_error"] = str(err)
        return result

    def _write_settle_delay(self) -> float:
        """Scale the post-write settle delay by how many icon units are present.

        Prefers the cached ``device_indices`` (the known bus topology from
        setup/reconfigure) over the last poll's ``data["icons"]``, since a
        slave that's temporarily offline shouldn't shrink the delay - the
        physical propagation chain is unchanged either way.
        """
        device_indices = self.inventory.get("device_indices")
        if device_indices:
            icon_count = len(device_indices)
        elif self.data:
            icon_count = len(self.data["icons"])
        else:
            icon_count = 1
        slave_count = max(0, icon_count - 1)
        return WRITE_SETTLE_BASE_DELAY + WRITE_SETTLE_PER_SLAVE_DELAY * slave_count

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch the full dataset from the controller."""
        try:
            return await self._async_fetch_and_cache()
        except IconModbusError as err:
            raise UpdateFailed(f"Error communicating with iCON: {err}") from err

    async def async_refresh_now(self) -> None:
        """Immediately re-read the full dataset and push it to entities.

        Used right after a write so entities reflect the change without
        waiting for the coordinator's normal refresh path, which can be
        debounced. A write to one thermostat can affect thermostats or
        relays on *other* icon controllers too - e.g. the H/C-master
        thermostat's preset can cascade to every zone, and a relay/valve can
        be wired to a different iCON than the thermostat driving it - so this
        always re-reads every controller rather than just the one written to.
        A full read is fast in practice (well under a second), but the
        controller itself needs a moment to fold the write into its
        read-only mirror registers, so this waits for
        :meth:`_write_settle_delay` first. Best effort: on failure this
        silently defers to the next scheduled poll.
        """
        await asyncio.sleep(self._write_settle_delay())
        try:
            data = await self._async_fetch_and_cache()
        except IconModbusError as err:
            _LOGGER.debug("Quick refresh failed: %s", err)
            return
        self.async_set_updated_data(data)
