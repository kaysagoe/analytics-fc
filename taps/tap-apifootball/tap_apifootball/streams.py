"""Stream type classes for tap-apifootball."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_apifootball.client import APIFootballStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class FixturesStream(APIFootballStream):
    """Define custom stream."""
    name = "fixtures"
    path = "/fixtures"
    primary_keys = None
    replication_key = None

    schema_filepath = SCHEMAS_DIR / "fixtures.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        param_keys = ['id', 'live', 'date', 'league', 'season', 'team', 'last', 'next', 'from',
                        'to', 'round', 'status', 'timezone']
        for key in param_keys:
            if key in self.config.keys():
                params[key] = self.config[key]

        return params

class StatisticsStream(APIFootballStream):
    """Define custom stream."""
    name = "statistics"
    path = "/fixtures/statistics"
    primary_keys = None
    replication_key = None

    schema_filepath = SCHEMAS_DIR / "statistics.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        param_keys = ['fixture', 'team', 'type']
        for key in param_keys:
            if key in self.config.keys():
                params[key] = self.config[key]

        return params

class EventsStream(APIFootballStream):
    """Define custom stream."""
    name = "events"
    path = "/fixtures/events"
    primary_keys = None
    replication_key = None

    schema_filepath = SCHEMAS_DIR / "events.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        param_keys = ['fixture', 'team', 'player', 'type']
        for key in param_keys:
            if key in self.config.keys():
                params[key] = self.config[key]

        return params

class LineupsStream(APIFootballStream):
    """Define custom stream."""
    name = "lineups"
    path = "/fixtures/lineups"
    primary_keys = None
    replication_key = None

    schema_filepath = SCHEMAS_DIR / "lineups.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        param_keys = ['fixture', 'team', 'player', 'type']
        for key in param_keys:
            if key in self.config.keys():
                params[key] = self.config[key]

        return params

class PlayerStatisticsStream(APIFootballStream):
    """Define custom stream."""
    name = "player_statistics"
    path = "/fixtures/players"
    primary_keys = None
    replication_key = None

    schema_filepath = SCHEMAS_DIR / "player_statistics.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        param_keys = ['fixture', 'team']
        for key in param_keys:
            if key in self.config.keys():
                params[key] = self.config[key]

        return params



class GroupsStream(APIFootballStream):
    """Define custom stream."""
    name = "groups"
    path = "/groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()
