"""APIFootball tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_apifootball.streams import (
    APIFootballStream,
    FixturesStream,
    StatisticsStream,
    EventsStream,
    LineupsStream,
    PlayerStatisticsStream
)

STREAM_TYPES = {
    'fixtures': FixturesStream,
    'statistics': StatisticsStream,
    'events': EventsStream,
    'lineups': LineupsStream,
    'player_statistics': PlayerStatisticsStream
}


class TapAPIFootball(Tap):
    """APIFootball tap class."""
    name = "tap-apifootball"

    config_jsonschema = {
        "type": "object",
        "properties": {
            "api_key": {
                "type": "string"
            },
            "id": {
                "type": "integer"
            },
            "live": {
                "enum": ["all", "id-id"]
            },
            "date": {
                "type": "string"
            },
            "fseason": {
                "type": "integer"
            },
            "team": {
                "type": "integer"
            },
            "last": {
                "type": "integer"   
            },
            "next": {
                "type": "integer"
            },
            "from": {
                "type": "string"
            },
            "to": {
                "type": "string"
            },
            "round": {
                "type": "string"
            },
            "status": {
                "type": "string"
            },
            "timezone": {
                "type": "string"
            },
            "fixture": {
                "type": "integer"
            },
            "team": {
                "type": "integer"
            },
            "type": {
                "type": "string"
            },
            "player": {
                "type": "integer"
            },
            "_stream": {
                "type": "string"
            }
        },
        "required": ["api_key"] 
    }

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if '_stream' in self.config.keys():
            return [STREAM_TYPES[self.config['_stream']](tap=self)]
        else:
            return [stream_class(tap=self) for stream_class in STREAM_TYPES.values()]
