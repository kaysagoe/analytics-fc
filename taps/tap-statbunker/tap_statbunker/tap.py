"""Statbunker tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_statbunker.streams import (
    StatbunkerStream,
    SeasonsStream,
    MatchesStream,
    MatchDetailsStream
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = {
    'seasons': SeasonsStream,
    'matches': MatchesStream,
    'match_details': MatchDetailsStream
}


class TapStatbunker(Tap):
    """Statbunker tap class."""
    name = "tap-statbunker"

    config_jsonschema = {
        'type': 'object',
        'properties': {
            '_stream': {
                'type': 'string'
            },
            'comp_id': {
                'type': 'integer'
            },
            'date': {
                'type': 'string'
            },
            'club_id': {
                'type': 'integer'
            },
            'match_id': {
                'type': 'integer'
            }
        }
    }

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if '_stream' in self.config:
            return [STREAM_TYPES[self.config['_stream']](tap=self)]
        else:
            return [stream_class(tap=self) for stream_class in STREAM_TYPES.values()]
