"""OnMI message handler for GOAT mower map data.

The GOAT series mowers (A3000 LiDAR, A1600 RTK, etc.) send full zone polygon
map data via chunked `onMI` MQTT ATR messages.

Each message is one chunk of LZMA-compressed JSON containing zone polygon
coordinates. Chunks must be reassembled before the map can be rendered.

Message format:
{
  "mid": "1",           - map ID
  "batid": "kehfmk",   - batch ID (groups chunks belonging to same map snapshot)
  "serial": "21",       - max chunk index (so serial+1 = total chunks)
  "index": "0",         - chunk index (0-based)
  "using": 1,
  "type": "-1",
  "info": "<b64lzma>",  - LZMA-compressed JSON for this chunk
  "infoSize": 112485    - total uncompressed size when all chunks concatenated
}

Decoded JSON format (array of zone entries):
[
  ["zone_id", "dock;x1,y1;x2,y2;...", "boundary;x1,y1;x2,y2;..."],
  ...
]

Coordinates are in mm from the dock position (dock = 0,0).
Zone names are not stored server-side for GOAT mowers.
"""

from __future__ import annotations

import base64
import lzma
import struct
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import orjson

from deebot_client.events.map import MapSetEvent, MapSetType, MapSubsetEvent
from deebot_client.logging_filter import get_logger
from deebot_client.message import HandlingResult, HandlingState, MessageBodyDataDict

if TYPE_CHECKING:
    from deebot_client.event_bus import EventBus

_LOGGER = get_logger(__name__)

# Module-level buffers for chunk accumulation
# Keyed by batid so concurrent map updates don't interfere
_chunk_buffer: dict[str, dict[int, bytes]] = defaultdict(dict)
_chunk_counts: dict[str, int] = {}


def _decompress_lzma_b64(b64_data: str) -> bytes:
    """Decompress a LZMA-compressed base64-encoded chunk."""
    data = base64.b64decode(b64_data)
    lzma_header = data[0:5]
    len_value = struct.unpack("<I", data[5:9])[0]
    filter_props = lzma._decode_filter_properties(lzma.FILTER_LZMA1, lzma_header)
    dec = lzma.LZMADecompressor(lzma.FORMAT_RAW, None, [filter_props])
    return dec.decompress(data[9:], len_value)


class OnMI(MessageBodyDataDict):
    """Handler for onMI GOAT mower map chunk messages."""

    NAME = "onMI"

    @classmethod
    def _handle_body_data_dict(
        cls, event_bus: EventBus, data: dict[str, Any]
    ) -> HandlingResult:
        """Accumulate onMI chunks and emit map events when complete."""
        batid = data.get("batid", "")
        serial = int(data.get("serial", 0))
        index = int(data.get("index", 0))
        mid = str(data.get("mid", "1"))
        info = data.get("info", "")
        total_chunks = serial + 1

        if not info or not batid:
            return HandlingResult.analyse()

        try:
            chunk = _decompress_lzma_b64(info)
        except Exception:
            _LOGGER.warning("Failed to decompress onMI chunk %d for batid=%s", index, batid)
            return HandlingResult.analyse()

        _chunk_buffer[batid][index] = chunk
        _chunk_counts[batid] = total_chunks

        _LOGGER.debug(
            "onMI chunk %d/%d received (batid=%s, map=%s)",
            index + 1, total_chunks, batid, mid,
        )

        if len(_chunk_buffer[batid]) < total_chunks:
            return HandlingResult.success()

        # All chunks received — reassemble
        try:
            full_bytes = b"".join(
                _chunk_buffer[batid][i] for i in range(total_chunks)
            )
        except KeyError:
            missing = [i for i in range(total_chunks) if i not in _chunk_buffer[batid]]
            _LOGGER.warning("onMI missing chunks %s for batid=%s", missing, batid)
            return HandlingResult.analyse()
        finally:
            del _chunk_buffer[batid]
            _chunk_counts.pop(batid, None)

        try:
            zones = orjson.loads(full_bytes)
        except Exception:
            _LOGGER.warning("Failed to parse reassembled onMI JSON for map %s", mid)
            return HandlingResult.analyse()

        _LOGGER.debug(
            "onMI reassembled %d zone entries for map %s", len(zones), mid
        )

        subset_ids: list[int] = []
        for zone in zones:
            if not isinstance(zone, list) or len(zone) < 3:
                continue

            try:
                zone_id = int(zone[0])
            except (ValueError, TypeError):
                continue

            # zone[2] = "zone_ref;x1,y1;x2,y2;..."
            boundary_str = zone[2] if len(zone) > 2 else ""
            parts = boundary_str.split(";")

            # Skip first element (zone reference), rest are coordinate pairs
            coord_pairs = [
                p for p in parts[1:] if "," in p
            ]
            coordinates = ";".join(coord_pairs)

            if not coordinates:
                continue

            event_bus.notify(
                MapSubsetEvent(
                    id=zone_id,
                    type=MapSetType.ROOMS,
                    coordinates=coordinates,
                    name="",
                )
            )
            subset_ids.append(zone_id)

        if subset_ids:
            event_bus.notify(MapSetEvent(MapSetType.ROOMS, subset_ids, mid))
            _LOGGER.debug(
                "Emitted MapSetEvent + %d MapSubsetEvents for map %s",
                len(subset_ids), mid,
            )

        return HandlingResult.success()
