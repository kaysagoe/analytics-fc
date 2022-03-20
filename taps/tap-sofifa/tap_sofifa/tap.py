"""SoFIFA tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_sofifa.streams import (
    SoFIFAStream,
    VersionsStream,
    ChangesStream,
    PlayerChangesStream,
    PlayerDetailStream
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = {
    'versions': VersionsStream,
    'changes': ChangesStream,
    'player_changes': PlayerChangesStream,
    'player_detail': PlayerDetailStream
}


class TapSoFIFA(Tap):
    """SoFIFA tap class."""
    name = "tap-sofifa"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = {
        'type': 'object',
        'properties': {
            'game_year': {
                'type': 'integer'
            },
            'league_id': {
                'type': 'integer'
            },
            'change_id': {
                'type': 'integer'
            },
            'player_id': {
                'type': 'integer'
            },
            '_stream': {
                'type': 'string'
            }
        }
    }

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if '_stream' in self.config:
            return [STREAM_TYPES[self.config['_stream']](tap=self)]
        else:
            return [stream_class(tap=self) for stream_class in STREAM_TYPES.values()]
