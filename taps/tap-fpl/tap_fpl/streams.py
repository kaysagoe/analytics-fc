"""Stream type classes for tap-fpl."""
import copy
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_fpl.client import FPLStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class EventsStream(FPLStream):
    name = "events"
    path = "/bootstrap-static"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "events.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json()['events']

class TeamsStream(FPLStream):
    name = "teams"
    path = "/bootstrap-static"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "teams.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json()['teams']

class ElementsStream(FPLStream):
    name = "elements"
    path = "/bootstrap-static"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "elements.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json()['elements']

class ElementTypesStream(FPLStream):
    name = "element-types"
    path = "/bootstrap-static"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "element_types.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json()['element_types']

class FixturesStream(FPLStream):
    name = "fixtures"
    path = "/fixtures"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "fixtures.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json()

class SelectionsStream(FPLStream):
    name = "selections"
    path = ''
    primary_keys = ["manager_id", "gameweek"]
    schema_filepath = SCHEMAS_DIR / "selection.json"
    records_jsonpath = "$" 

    def get_url(self, context: Optional[dict], manager_id, gameweek) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        path = "".join(['/entry/', str(manager_id), '/event/', str(gameweek), '/picks'])
        url = "".join([self.url_base, path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = "".join(["{", k, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        return url

    def prepare_request(
        self, context: Optional[dict], manager_id, gameweek
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context, manager_id= manager_id, gameweek= gameweek)
        params: dict = {}
        request_data = None
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
            params.update(authenticator.auth_params or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )
        return request
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        decorated_request = self.request_decorator(self._request)

        for gameweek in self.config['gameweeks']:
            for manager_id in self.config['managers']:
                prepared_request = self.prepare_request(context, manager_id, gameweek)
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp, manager_id, gameweek):
                    yield row

    
    def parse_response(self, response: requests.Response, manager_id, gameweek) -> Iterable[dict]:
        edited_response = response.json()
        edited_response['manager_id'] = manager_id
        edited_response['gameweek'] = gameweek
        yield from extract_jsonpath(self.records_jsonpath, input=edited_response)

class StandingsStream(FPLStream):
    name = "standings"
    primary_keys = ["league_id"]
    replication_key = "last_updated_data"
    schema_filepath = SCHEMAS_DIR / "standings.json"
    records_jsonpath = "$" 

    @property
    def path(self):
        return f'/leagues-classic/{self.config["league_id"]}/standings'

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

class PlayerDetailsStream(FPLStream):
    name = "player_details"
    path = ''
    schema_filepath = SCHEMAS_DIR / "player_details.json"
    records_jsonpath = "$" 

    def get_url(self, context: Optional[dict], player_id) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        path = f'/element-summary/{player_id}'
        url = "".join([self.url_base, path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = "".join(["{", k, "}"])
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        return url

    def prepare_request(
        self, context: Optional[dict], player_id
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context, player_id)
        params: dict = {}
        request_data = None
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
            params.update(authenticator.auth_params or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )
        return request
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        decorated_request = self.request_decorator(self._request)

        for player_id in self.config['players']:
            prepared_request = self.prepare_request(context, player_id)
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp, player_id):
                yield row

    
    def parse_response(self, response: requests.Response, player_id) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())


class GroupsStream(FPLStream):
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
