"""Test script for Deebot client."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import time
from typing import Any

import aiohttp

from deebot_client.api_client import ApiClient
from deebot_client.authentication import Authenticator, create_rest_config
from deebot_client.commands.json.charge import Charge
from deebot_client.commands.json.clean import (  # type: ignore[attr-defined]
    Clean,
    CleanAction,
)
from deebot_client.commands.json.map import GetMajorMap
from deebot_client.device import Device
from deebot_client.events import (
    AdvancedModeEvent,
    AvailabilityEvent,
    BatteryEvent,
    CachedMapInfoEvent,
    CarpetAutoFanBoostEvent,
    ChildLockEvent,
    CleanCountEvent,
    CleanLogEvent,
    CleanPreferenceEvent,
    ContinuousCleaningEvent,
    CustomCommandEvent,
    EfficiencyModeEvent,
    ErrorEvent,
    Event,
    FanSpeedEvent,
    LifeSpanEvent,
    MajorMapEvent,
    MapChangedEvent,
    MapTraceEvent,
    MultimapStateEvent,
    NetworkInfoEvent,
    OtaEvent,
    PositionsEvent,
    ReportStatsEvent,
    RoomsEvent,
    StateEvent,
    StationEvent,
    StatsEvent,
    SweepModeEvent,
    TotalStatsEvent,
    TrueDetectEvent,
    VoiceAssistantStateEvent,
    VolumeEvent,
    WorkModeEvent,
    auto_empty,
)
from deebot_client.mqtt_client import MqttClient, create_mqtt_config
from deebot_client.util import md5

_LOGGER = logging.getLogger(__name__)

device_id = md5(str(time.time()))
account_id = "Schrott.Micha@web.de"
password_hash = md5("EcoVacs74*!")
country = "DE"


def _setup_event_handlers() -> dict[Any, Any]:  # noqa: C901, PLR0915
    """Set up event handler functions."""

    async def on_value_event(event: Event) -> None:
        _LOGGER.info(
            "%s: value=%s", event.__class__.__name__, getattr(event, "value", None)
        )

    async def on_enable_event(event: Event) -> None:
        _LOGGER.info(
            "%s: enabled=%s",
            event.__class__.__name__,
            getattr(event, "enabled", None),
        )

    async def on_available_event(event: Event) -> None:
        _LOGGER.info(
            "%s: available=%s",
            event.__class__.__name__,
            getattr(event, "available", None),
        )

    async def on_maps_event(event: Event) -> None:
        _LOGGER.info(
            "%s: maps=%s", event.__class__.__name__, getattr(event, "maps", None)
        )

    async def on_count_event(event: Event) -> None:
        _LOGGER.info(
            "%s: count=%s", event.__class__.__name__, getattr(event, "count", None)
        )

    async def on_logs_event(event: Event) -> None:
        _LOGGER.info(
            "%s: logs=%s", event.__class__.__name__, getattr(event, "logs", None)
        )

    async def on_custom_command_event(event: Event) -> None:
        _LOGGER.info(
            "%s: name=%s - response=%s",
            event.__class__.__name__,
            getattr(event, "name", None),
            getattr(event, "response", None),
        )

    async def on_efficiency_event(event: Event) -> None:
        _LOGGER.info(
            "%s: efficiency=%s",
            event.__class__.__name__,
            getattr(event, "efficiency", None),
        )

    async def on_error_event(event: Event) -> None:
        _LOGGER.info(
            "%s: code=%s - description=%s",
            event.__class__.__name__,
            getattr(event, "code", None),
            getattr(event, "description", None),
        )

    async def on_speed_event(event: Event) -> None:
        _LOGGER.info(
            "%s: speed=%s", event.__class__.__name__, getattr(event, "speed", None)
        )

    async def on_lifespan_event(event: Event) -> None:
        _LOGGER.info(
            "%s: type=%s - percent=%s - remaining=%s",
            event.__class__.__name__,
            getattr(event, "type", None),
            getattr(event, "percent", None),
            getattr(event, "remaining", None),
        )

    async def on_major_map_event(event: Event) -> None:
        _LOGGER.info(
            "%s: map_id=%s - values=%s - requested=%s",
            event.__class__.__name__,
            getattr(event, "map_id", None),
            getattr(event, "values", None),
            getattr(event, "requested", None),
        )

    async def on_when_event(event: Event) -> None:
        _LOGGER.info(
            "%s: when=%s", event.__class__.__name__, getattr(event, "when", None)
        )

    async def on_map_trace_event(event: Event) -> None:
        _LOGGER.info(
            "%s: start=%s - total=%s - data=%s",
            event.__class__.__name__,
            getattr(event, "start", None),
            getattr(event, "total", None),
            getattr(event, "data", None),
        )

    async def on_network_info_event(event: Event) -> None:
        _LOGGER.info(
            "%s: ip=%s - ssid=%s - rssi=%s - mac=%s",
            event.__class__.__name__,
            getattr(event, "ip", None),
            getattr(event, "ssid", None),
            getattr(event, "rssi", None),
            getattr(event, "mac", None),
        )

    async def on_ota_event(event: Event) -> None:
        _LOGGER.info(
            "%s: status=%s - progress=%s",
            event.__class__.__name__,
            getattr(event, "status", None),
            getattr(event, "progress", None),
        )

    async def on_positions_event(event: Event) -> None:
        _LOGGER.info(
            "%s: positions=%s",
            event.__class__.__name__,
            getattr(event, "positions", None),
        )

    async def on_report_stats_event(event: Event) -> None:
        _LOGGER.info(
            "%s: cleaning_id=%s - status=%s - content=%s",
            event.__class__.__name__,
            getattr(event, "cleaning_id", None),
            getattr(event, "status", None),
            getattr(event, "content", None),
        )

    async def on_rooms_event(event: Event) -> None:
        _LOGGER.info(
            "%s: rooms=%s", event.__class__.__name__, getattr(event, "rooms", None)
        )

    async def on_state_event(event: Event) -> None:
        state = getattr(event, "state", None)
        _LOGGER.info("%s: state=%s", event.__class__.__name__, state)

    async def on_stats_event(event: Event) -> None:
        _LOGGER.info(
            "%s: area=%s - time=%s - type=%s",
            event.__class__.__name__,
            getattr(event, "area", None),
            getattr(event, "time", None),
            getattr(event, "type", None),
        )

    async def on_total_stats_event(event: Event) -> None:
        _LOGGER.info(
            "%s: area=%s - time=%s - cleanings=%s",
            event.__class__.__name__,
            getattr(event, "area", None),
            getattr(event, "time", None),
            getattr(event, "cleanings", None),
        )

    async def on_volume_event(event: Event) -> None:
        _LOGGER.info(
            "%s: volume=%s - maximum=%s",
            event.__class__.__name__,
            getattr(event, "volume", None),
            getattr(event, "maximum", None),
        )

    async def on_work_mode_event(event: Event) -> None:
        _LOGGER.info(
            "%s: mode=%s", event.__class__.__name__, getattr(event, "mode", None)
        )

    async def on_auto_empty_event(event: Event) -> None:
        _LOGGER.info(
            "%s: enabled=%s, frequency=%s",
            event.__class__.__name__,
            getattr(event, "enabled", None),
            getattr(event, "frequency", None),
        )

    return {
        AdvancedModeEvent: on_enable_event,
        AvailabilityEvent: on_available_event,
        BatteryEvent: on_value_event,
        CachedMapInfoEvent: on_maps_event,
        CarpetAutoFanBoostEvent: on_enable_event,
        ChildLockEvent: on_enable_event,
        CleanCountEvent: on_count_event,
        CleanLogEvent: on_logs_event,
        CleanPreferenceEvent: on_enable_event,
        ContinuousCleaningEvent: on_enable_event,
        CustomCommandEvent: on_custom_command_event,
        EfficiencyModeEvent: on_efficiency_event,
        ErrorEvent: on_error_event,
        FanSpeedEvent: on_speed_event,
        LifeSpanEvent: on_lifespan_event,
        MajorMapEvent: on_major_map_event,
        MapChangedEvent: on_when_event,
        MapTraceEvent: on_map_trace_event,
        MultimapStateEvent: on_enable_event,
        NetworkInfoEvent: on_network_info_event,
        OtaEvent: on_ota_event,
        PositionsEvent: on_positions_event,
        ReportStatsEvent: on_report_stats_event,
        RoomsEvent: on_rooms_event,
        StateEvent: on_state_event,
        StationEvent: on_state_event,
        StatsEvent: on_stats_event,
        SweepModeEvent: on_enable_event,
        TotalStatsEvent: on_total_stats_event,
        TrueDetectEvent: on_enable_event,
        VoiceAssistantStateEvent: on_enable_event,
        VolumeEvent: on_volume_event,
        WorkModeEvent: on_work_mode_event,
        auto_empty: on_auto_empty_event,
    }


async def amain() -> None:
    """Run the Deebot client test."""
    async with aiohttp.ClientSession() as session:
        logging.basicConfig(
            level=logging.DEBUG,
            filename="test.log",
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        _LOGGER.info("\n\n\n\nStarting bot...")

        rest_config = create_rest_config(
            session, device_id=device_id, alpha_2_country=country
        )

        authenticator = Authenticator(rest_config, account_id, password_hash)
        api_client = ApiClient(authenticator)

        devices_ = await api_client.get_devices()

        _LOGGER.info("Found %d MQTT devices.", len(devices_.mqtt))
        if len(devices_.mqtt) == 0:
            _LOGGER.error("No MQTT devices found, exiting.")
            return
        _LOGGER.info(
            "Using device: %s (%s)", devices_.mqtt[0].api, devices_.mqtt[0].static
        )
        bot = Device(devices_.mqtt[0], authenticator)

        mqtt_config = create_mqtt_config(device_id=device_id, country=country)
        mqtt = MqttClient(mqtt_config, authenticator)
        await bot.initialize(mqtt)
        _LOGGER.info("Bot initialized.")

        # Subscribe to all events
        events = _setup_event_handlers()
        _LOGGER.info("Subscribing to all events...")
        for event_class, handler in events.items():
            if isinstance(event_class, type):
                _LOGGER.info("Subscribing to %s...", event_class.__name__)
                bot.events.subscribe(event_class, handler)

        # Execute commands
        _LOGGER.info("Starting cleaning...")
        await bot.execute_command(Clean(CleanAction.START))
        await asyncio.sleep(90)  # Wait for...
        _LOGGER.info("Returning to charge...")
        await bot.execute_command(Charge())

        await bot.execute_command(GetMajorMap())
        if bot.map is not None:
            bot.map.refresh()
            _LOGGER.info("Map refreshed.")
        await asyncio.sleep(10)
        if bot.map is not None and (svg := bot.map.get_svg_map()):
            Path("map.svg").write_text(svg)
            _LOGGER.info("Map SVG saved to map.svg.")
        else:
            _LOGGER.info("No map SVG available.")

        _LOGGER.info("Bot started.")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(amain())
    except KeyboardInterrupt:
        pass
    finally:
        _LOGGER.info("Bot stopped.")
        loop.close()
