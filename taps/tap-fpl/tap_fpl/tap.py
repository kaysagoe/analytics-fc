"""FPL tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from pathlib import Path

from tap_fpl.streams import (
    FPLStream,
    EventsStream,
    TeamsStream,
    ElementsStream,
    ElementTypesStream,
    FixturesStream,
    SelectionsStream,
    StandingsStream,
    PlayerDetailsStream
)

STREAM_TYPES = {
    'events': EventsStream,
    'teams': TeamsStream,
    'elements': ElementsStream,
    'element-types': ElementTypesStream,
    'fixtures': FixturesStream,
    'selections': SelectionsStream,
    'standings': StandingsStream,
    'player-details': PlayerDetailsStream
}

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TapFPL(Tap):
    """FPL tap class."""
    name = "tap-fpl"

    config_jsonschema = {
        "type": "object",
        "properties": {
            "managers": {
                "type": "array",
                "items": {
                    "type": "integer"
                },
                "default": [] 
            },
            "gameweeks": {
                "type": "array",
                "items": {
                    "type": "integer"
                },
                "default": []
            },
            "players": {
                "type": "array",
                "items": {
                    "type": "integer"
                },
                "default": []
            },
            "_stream": {
                'type': 'string'
            }
        },
        "required": ["managers", "gameweeks", "players"]
    }

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if '_stream' in self.config.keys():
            return [STREAM_TYPES[self.config['_stream']](tap=self)]
        else:
            return [stream_class(tap=self) for stream_class in STREAM_TYPES.values()]