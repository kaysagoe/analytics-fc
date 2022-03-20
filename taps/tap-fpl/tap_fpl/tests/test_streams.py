import sqlite3

from pytest import fixture, raises
from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime, timedelta
from tap_fpl.tap import TapFPL
from tap_fpl.streams import PlayerDetailsStream
from singer_sdk.testing import tap_to_target_sync_test
from target_tester.target import TargetTester
from copy import deepcopy

@fixture
def mock_session():
    mock_response = Mock()
    mock_response.elapsed = timedelta(seconds=0)
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_session = Mock()
    mock_session.send.return_value = mock_response
    return mock_session

@fixture
def target():
    return TargetTester()

class TestEventsStream:
    @patch('singer_sdk.streams.rest.requests')
    def test_load_finished_game_week_event(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'name': 'Gameweek 1',
                "deadline_time": "2000-01-01T00:00:00Z",
                "average_entry_score": 1,
                "finished": True,
                "data_checked": True,
                "highest_scoring_entry": 1111111,
                "deadline_time_epoch": 0000000000,
                "deadline_time_game_offset": 0,
                "highest_score": 1,
                "is_previous": False,
                "is_current": False,
                "is_next": False,
                "cup_leagues_created": False,
                "h2h_ko_matches_created": False,
                "chip_plays": [
                    {
                        "chip_name": "bboost",
                        "num_played": 1
                    }
                ],
                "most_selected": 1,
                "most_transferred_in": 1,
                "top_element": 1,
                "top_element_info": {
                    "id": 1,
                    "points": 1
                },
                "transfers_made": 0,
                "most_captained": 1,
                "most_vice_captained": 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'events': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'events'
        })
        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_unfinished_game_week_event(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'name': 'Gameweek 1',
                "deadline_time": "2000-01-01T00:00:00Z",
                "average_entry_score": 1,
                "finished": False,
                "data_checked": False,
                "highest_scoring_entry": None,
                "deadline_time_epoch": 0000000000,
                "deadline_time_game_offset": 0,
                "highest_score": None,
                "is_previous": False,
                "is_current": False,
                "is_next": False,
                "cup_leagues_created": False,
                "h2h_ko_matches_created": False,
                "chip_plays": [],
                "most_selected": None,
                "most_transferred_in": None,
                "top_element": None,
                "top_element_info": None,
                "transfers_made": 0,
                "most_captained": None,
                "most_vice_captained": None
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'events': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'events'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_ongoing_game_week_event(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'name': 'Gameweek 1',
                "deadline_time": "2000-01-01T00:00:00Z",
                "average_entry_score": 1,
                "finished": False,
                "data_checked": False,
                "highest_scoring_entry": 1111111,
                "deadline_time_epoch": 0000000000,
                "deadline_time_game_offset": 0,
                "highest_score": 1,
                "is_previous": False,
                "is_current": True,
                "is_next": False,
                "cup_leagues_created": False,
                "h2h_ko_matches_created": False,
                "chip_plays": [
                    {
                        "chip_name": "bboost",
                        "num_played": 1
                    }
                ],
                "most_selected": 1,
                "most_transferred_in": 1,
                "top_element": 1,
                "top_element_info": {
                    "id": 1,
                    "points": 1
                },
                "transfers_made": 0,
                "most_captained": 1,
                "most_vice_captained": 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'events': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'events'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual
    
    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_game_week_events(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'name': 'Gameweek 1',
                "deadline_time": "2000-01-01T00:00:00Z",
                "average_entry_score": 1,
                "finished": False,
                "data_checked": False,
                "highest_scoring_entry": 1111111,
                "deadline_time_epoch": 0000000000,
                "deadline_time_game_offset": 0,
                "highest_score": 1,
                "is_previous": False,
                "is_current": True,
                "is_next": False,
                "cup_leagues_created": False,
                "h2h_ko_matches_created": False,
                "chip_plays": [
                    {
                        "chip_name": "bboost",
                        "num_played": 1
                    }
                ],
                "most_selected": 1,
                "most_transferred_in": 1,
                "top_element": 1,
                "top_element_info": {
                    "id": 1,
                    "points": 1
                },
                "transfers_made": 0,
                "most_captained": 1,
                "most_vice_captained": 1
            },
            {
                'id': 1,
                'name': 'Gameweek 1',
                "deadline_time": "2000-01-01T00:00:00Z",
                "average_entry_score": 1,
                "finished": False,
                "data_checked": False,
                "highest_scoring_entry": None,
                "deadline_time_epoch": 0000000000,
                "deadline_time_game_offset": 0,
                "highest_score": None,
                "is_previous": False,
                "is_current": False,
                "is_next": False,
                "cup_leagues_created": False,
                "h2h_ko_matches_created": False,
                "chip_plays": [],
                "most_selected": None,
                "most_transferred_in": None,
                "top_element": None,
                "top_element_info": None,
                "transfers_made": 0,
                "most_captained": None,
                "most_vice_captained": None
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'events': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'events'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': '1',
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'events': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'events'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)
    
class TestTeamsStream:
    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_team(self, mock_requests, mock_session, target):
        expected = [
            {
                'code': 1,
                'draw': 0,
                'form': None,
                'id': 1,
                'loss': 0,
                'name': 'Test Teams',
                'played': 0,
                'points': 0,
                'position': 0,
                'short_name': 'TEA',
                'team_division': None,
                'unavailable': False,
                'win': 0,
                'strength_overall_home': 0,
                'strength_overall_away': 0,
                'strength_attack_home': 0,
                'strength_attack_away': 0,
                'strength_defence_home': 0,
                'strength_defence_away': 0,
                'pulse_id': 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'teams': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'teams'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_teams(self, mock_requests, mock_session, target):
        expected = [
            {
                'code': 1,
                'draw': 0,
                'form': None,
                'id': 1,
                'loss': 0,
                'name': 'Test Team A',
                'played': 0,
                'points': 0,
                'position': 0,
                'short_name': 'TEA',
                'team_division': None,
                'unavailable': False,
                'win': 0,
                'strength_overall_home': 0,
                'strength_overall_away': 0,
                'strength_attack_home': 0,
                'strength_attack_away': 0,
                'strength_defence_home': 0,
                'strength_defence_away': 0,
                'pulse_id': 1
            },
            {
                'code': 2,
                'draw': 0,
                'form': None,
                'id': 2,
                'loss': 0,
                'name': 'Test Team B',
                'played': 0,
                'points': 0,
                'position': 0,
                'short_name': 'TEB',
                'team_division': None,
                'unavailable': False,
                'win': 0,
                'strength_overall_home': 0,
                'strength_overall_away': 0,
                'strength_attack_home': 0,
                'strength_attack_away': 0,
                'strength_defence_home': 0,
                'strength_defence_away': 0,
                'pulse_id': 2
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'teams': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'teams'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': '1',
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'teams': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'teams'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

class TestElementsStream:
    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_element(self, mock_requests, mock_session, target):
        expected = [
            {
                'chance_of_playing_next_round': 100,
                'chance_of_playing_this_round': 100,
                'code': 1,
                'cost_change_event': 0,
                'cost_change_event_fall': 0,
                'cost_change_start': 0,
                'cost_change_start_fall': 0,
                'dreamteam_count': 0,
                'element_type': 1,
                'ep_next': '0.0',
                'ep_this': '1.0',
                'event_points': 0,
                'first_name': 'John',
                'form': '0.0',
                'id': 1,
                'in_dreamteam': False,
                'news': '',
                'news_added': '2000-01-01T00:00:00.000000Z',
                'now_cost': 0,
                'photo': 'photo.jpg',
                'points_per_game': '0.0',
                'second_name': 'Doe',
                'selected_by_percent': '0.0',
                'special': False,
                'squad_number': None,
                'status': 'a',
                'team': 1,
                'team_code': 1,
                'total_points': 0,
                'transfers_in': 0,
                'transfers_in_event': 0,
                'transfers_out': 0,
                'transfers_out_event': 0,
                'value_form': '0.0',
                'value_season': '0.0',
                'web_name': 'Doe',
                'minutes': 0,
                'goals_scored': 0,
                'assists': 0,
                'clean_sheets': 0,
                'goals_conceded': 0,
                'own_goals': 0,
                'penalties_saved': 0,
                'penalties_missed': 0,
                'yellow_cards': 0,
                'red_cards': 0,
                'saves': 0,
                'bonus': 0,
                'bps': 0,
                'influence': '0.0',
                'creativity': '0.0',
                'threat': '0.0',
                'ict_index': '0.0',
                'influence_rank': 0,
                'influence_rank_type': 0,
                'creativity_rank': 0,
                'creativity_rank_type': 0,
                'threat_rank': 0,
                'threat_rank_type': 0,
                'corners_and_indirect_freekicks_order': None,
                'corners_and_indirect_freekicks_text': '',
                'direct_freekicks_order': None,
                'direct_freekicks_text': '',
                'penalties_order': None,
                'penalties_text': ''
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'elements': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'elements'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_elements(self, mock_requests, mock_session, target):
        expected = [
            {
                'chance_of_playing_next_round': 100,
                'chance_of_playing_this_round': 100,
                'code': 1,
                'cost_change_event': 0,
                'cost_change_event_fall': 0,
                'cost_change_start': 0,
                'cost_change_start_fall': 0,
                'dreamteam_count': 0,
                'element_type': 1,
                'ep_next': '0.0',
                'ep_this': '1.0',
                'event_points': 0,
                'first_name': 'John',
                'form': '0.0',
                'id': 2,
                'in_dreamteam': False,
                'news': '',
                'news_added': '2000-01-01T00:00:00.000000Z',
                'now_cost': 0,
                'photo': 'photo.jpg',
                'points_per_game': '0.0',
                'second_name': 'Appleseed',
                'selected_by_percent': '0.0',
                'special': False,
                'squad_number': None,
                'status': 'a',
                'team': 1,
                'team_code': 1,
                'total_points': 0,
                'transfers_in': 0,
                'transfers_in_event': 0,
                'transfers_out': 0,
                'transfers_out_event': 0,
                'value_form': '0.0',
                'value_season': '0.0',
                'web_name': 'Appleseed',
                'minutes': 0,
                'goals_scored': 0,
                'assists': 0,
                'clean_sheets': 0,
                'goals_conceded': 0,
                'own_goals': 0,
                'penalties_saved': 0,
                'penalties_missed': 0,
                'yellow_cards': 0,
                'red_cards': 0,
                'saves': 0,
                'bonus': 0,
                'bps': 0,
                'influence': '0.0',
                'creativity': '0.0',
                'threat': '0.0',
                'ict_index': '0.0',
                'influence_rank': 0,
                'influence_rank_type': 0,
                'creativity_rank': 0,
                'creativity_rank_type': 0,
                'threat_rank': 0,
                'threat_rank_type': 0,
                'corners_and_indirect_freekicks_order': None,
                'corners_and_indirect_freekicks_text': '',
                'direct_freekicks_order': None,
                'direct_freekicks_text': '',
                'penalties_order': None,
                'penalties_text': ''
            },
            {
                'chance_of_playing_next_round': 100,
                'chance_of_playing_this_round': 100,
                'code': 1,
                'cost_change_event': 0,
                'cost_change_event_fall': 0,
                'cost_change_start': 0,
                'cost_change_start_fall': 0,
                'dreamteam_count': 0,
                'element_type': 1,
                'ep_next': '0.0',
                'ep_this': '1.0',
                'event_points': 0,
                'first_name': 'John',
                'form': '0.0',
                'id': 1,
                'in_dreamteam': False,
                'news': '',
                'news_added': '2000-01-01T00:00:00.000000Z',
                'now_cost': 0,
                'photo': 'photo.jpg',
                'points_per_game': '0.0',
                'second_name': 'Doe',
                'selected_by_percent': '0.0',
                'special': False,
                'squad_number': None,
                'status': 'a',
                'team': 1,
                'team_code': 1,
                'total_points': 0,
                'transfers_in': 0,
                'transfers_in_event': 0,
                'transfers_out': 0,
                'transfers_out_event': 0,
                'value_form': '0.0',
                'value_season': '0.0',
                'web_name': 'Doe',
                'minutes': 0,
                'goals_scored': 0,
                'assists': 0,
                'clean_sheets': 0,
                'goals_conceded': 0,
                'own_goals': 0,
                'penalties_saved': 0,
                'penalties_missed': 0,
                'yellow_cards': 0,
                'red_cards': 0,
                'saves': 0,
                'bonus': 0,
                'bps': 0,
                'influence': '0.0',
                'creativity': '0.0',
                'threat': '0.0',
                'ict_index': '0.0',
                'influence_rank': 0,
                'influence_rank_type': 0,
                'creativity_rank': 0,
                'creativity_rank_type': 0,
                'threat_rank': 0,
                'threat_rank_type': 0,
                'corners_and_indirect_freekicks_order': None,
                'corners_and_indirect_freekicks_text': '',
                'direct_freekicks_order': None,
                'direct_freekicks_text': '',
                'penalties_order': None,
                'penalties_text': ''
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'elements': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'elements'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': '1',
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'elements': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'elements'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

class TestElementTypesStream:
    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_element_type(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'plural_name': 'Positions',
                'plural_name_short': 'PST',
                'singular_name': 'Position',
                'singular_name_short': 'PST',
                'squad_select': 1,
                'squad_min_play': 1,
                'squad_max_play': 1,
                'ui_shirt_specific': False,
                'sub_positions_locked': [1],
                'element_count': 0
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'element_types': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'element-types'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_element_types(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': 1,
                'plural_name': 'Positions',
                'plural_name_short': 'PST',
                'singular_name': 'Position',
                'singular_name_short': 'PST',
                'squad_select': 1,
                'squad_min_play': 1,
                'squad_max_play': 1,
                'ui_shirt_specific': False,
                'sub_positions_locked': [1],
                'element_count': 0
            },
            {
                'id': 2,
                'plural_name': 'Positions',
                'plural_name_short': 'PST',
                'singular_name': 'Position',
                'singular_name_short': 'PST',
                'squad_select': 1,
                'squad_min_play': 1,
                'squad_max_play': 1,
                'ui_shirt_specific': False,
                'sub_positions_locked': [],
                'element_count': 0
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'element_types': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'element-types'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': '1',
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'element-types': deepcopy(expected)}

        tap = TapFPL(config={
            '_stream': 'element-types'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

class TestFixturesStream:
    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_finished_fixture(self, mock_requests, mock_session, target):
        expected = [
            {
                'code': 1,
                'event': 1,
                'finished': True,
                'finished_provisional': True,
                'id': 1,
                'kickoff_time': '2021-01-01T00:00:00.00Z',
                'minutes': 90,
                'provisional_start_time': False,
                'started': True,
                'team_a': 1,
                'team_a_score': 1,
                'team_h': 1,
                'team_h_score': 1,
                'stats': [
                    {
                        'identifier': 'goals_scored',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'assists',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'own_goals',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'penalties_saved',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'penalties_missed',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'yellow_cards',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'red_cards',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'saves',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'bonus',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    },
                    {
                        'identifier': 'bps',
                        'a': [{'value': 1, 'element': 1}],
                        'h': [{'value': 1, 'element': 2}]
                    }
                ],
                'team_h_difficulty': 1,
                'team_a_difficulty': 2,
                'pulse_id': 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'fixtures'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_unfinished_fixture(self, mock_requests, mock_session, target):
        expected = [
            {
                'code': 1,
                'event': 1,
                'finished': False,
                'finished_provisional': False,
                'id': 1,
                'kickoff_time': '2021-01-01T00:00:00.00Z',
                'minutes': 0,
                'provisional_start_time': False,
                'started': False,
                'team_a': 1,
                'team_a_score': None,
                'team_h': 1,
                'team_h_score': None,
                'stats': [],
                'team_h_difficulty': 1,
                'team_a_difficulty': 2,
                'pulse_id': 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'fixtures'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_fixture(self, mock_requests, mock_session, target):
        expected = [
            {
                'code': 1,
                'event': 1,
                'finished': False,
                'finished_provisional': False,
                'id': 1,
                'kickoff_time': '2021-01-01T00:00:00.00Z',
                'minutes': 0,
                'provisional_start_time': False,
                'started': False,
                'team_a': 1,
                'team_a_score': None,
                'team_h': 1,
                'team_h_score': None,
                'stats': [],
                'team_h_difficulty': 1,
                'team_a_difficulty': 2,
                'pulse_id': 1
            },
            {
                'code': 2,
                'event': 1,
                'finished': False,
                'finished_provisional': False,
                'id': 2,
                'kickoff_time': '2021-01-01T00:00:00.00Z',
                'minutes': 0,
                'provisional_start_time': False,
                'started': False,
                'team_a': 1,
                'team_a_score': None,
                'team_h': 1,
                'team_h_score': None,
                'stats': [],
                'team_h_difficulty': 1,
                'team_a_difficulty': 2,
                'pulse_id': 1
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'fixtures'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual
    
    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = [
            {
                'id': '1',
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'fixtures'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

class TestPlayerDetailsStream:
    def test_compute_url_for_player_id(self):
        expected = 'https://fantasy.premierleague.com/api/element-summary/1'

        tap = TapFPL(config={
            '_stream': 'player-details'
        })
        assert expected == tap.discover_streams()[0].get_url({}, player_id = 1)

    @patch('singer_sdk.streams.rest.requests')
    def test_load_single_player(self, mock_requests, mock_session, target): 
        expected = [
            {
                'fixtures': [
                    {
                        'id': 1,
                        'code': 1,
                        'team_h': 1,
                        'team_h_score': None,
                        'team_a': 1,
                        'team_a_score': None,
                        'event': 1,
                        'finished': False,
                        'minutes': 0,
                        'provisional_start_time': False,
                        'kick_off_time': '2022-01-01T00:00:00Z',
                        'event_name': 'Gameweek 1',
                        'is_home': False,
                        'difficulty': 1
                    }
                ],
                'history': [
                    {
                        'element': 1,
                        'fixture': 1,
                        'opponent_team': 1,
                        'total_points': 0,
                        'kickoff_time': '2022-01-01T00:00:00Z',
                        'was_home': False,
                        'team_h_score': 0,
                        'team_a_score': 0,
                        'round': 1,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0',
                        'value': 0,
                        'transfers_balance': 0,
                        'selected': 0,
                        'transfers_in': 0,
                        'transfers_out': 0  
                    }
                ],
                'history_past': [
                    {
                        'season_name': '1900/01',
                        'element_code': 1,
                        'start_cost': 0,
                        'end_cost': 0,
                        'total_points': 87,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0'
                    }
                ]
            }
        ]
        
        response = deepcopy(expected)[0]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = response

        tap = TapFPL(config={
            '_stream': 'player-details',
            'players': [1]
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_load_multiple_players(self, mock_requests, mock_session, target):
        expected = [
            {
                'fixtures': [
                    {
                        'id': 1,
                        'code': 1,
                        'team_h': 1,
                        'team_h_score': None,
                        'team_a': 1,
                        'team_a_score': None,
                        'event': 1,
                        'finished': False,
                        'minutes': 0,
                        'provisional_start_time': False,
                        'kick_off_time': '2022-01-01T00:00:00Z',
                        'event_name': 'Gameweek 1',
                        'is_home': False,
                        'difficulty': 1
                    }
                ],
                'history': [
                    {
                        'element': 1,
                        'fixture': 1,
                        'opponent_team': 1,
                        'total_points': 0,
                        'kickoff_time': '2022-01-01T00:00:00Z',
                        'was_home': False,
                        'team_h_score': 0,
                        'team_a_score': 0,
                        'round': 1,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0',
                        'value': 0,
                        'transfers_balance': 0,
                        'selected': 0,
                        'transfers_in': 0,
                        'transfers_out': 0  
                    }
                ],
                'history_past': [
                    {
                        'season_name': '1900/01',
                        'element_code': 1,
                        'start_cost': 0,
                        'end_cost': 0,
                        'total_points': 87,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0'
                    }
                ]
            },
            {
                'fixtures': [
                    {
                        'id': 2,
                        'code': 1,
                        'team_h': 1,
                        'team_h_score': None,
                        'team_a': 1,
                        'team_a_score': None,
                        'event': 1,
                        'finished': False,
                        'minutes': 0,
                        'provisional_start_time': False,
                        'kick_off_time': '2022-01-01T00:00:00Z',
                        'event_name': 'Gameweek 1',
                        'is_home': False,
                        'difficulty': 1
                    }
                ],
                'history': [
                    {
                        'element': 2,
                        'fixture': 1,
                        'opponent_team': 1,
                        'total_points': 0,
                        'kickoff_time': '2022-01-01T00:00:00Z',
                        'was_home': False,
                        'team_h_score': 0,
                        'team_a_score': 0,
                        'round': 1,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0',
                        'value': 0,
                        'transfers_balance': 0,
                        'selected': 0,
                        'transfers_in': 0,
                        'transfers_out': 0  
                    }
                ],
                'history_past': [
                    {
                        'season_name': '1900/01',
                        'element_code': 2,
                        'start_cost': 0,
                        'end_cost': 0,
                        'total_points': 87,
                        'minutes': 0,
                        'goals_scored': 0,
                        'assists': 0,
                        'clean_sheets': 0,
                        'goals_conceded': 0,
                        'own_goals': 0,
                        'penalties_saved': 0,
                        'penalties_missed': 0,
                        'yellow_cards': 0,
                        'red_cards': 0,
                        'saves': 0,
                        'bonus': 0,
                        'bps': 0,
                        'influence': '0.0',
                        'creativity': '0.0',
                        'threat': '0.0',
                        'ict_index': '0.0'
                    }
                ]
            }
        ]
        
        response = deepcopy(expected)

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.side_effect = response

        tap = TapFPL(config={
            '_stream': 'player-details',
            'players': [1,2]
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = {'fixtures': '1'}
        

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = expected

        tap = TapFPL(config={
            '_stream': 'player-details',
            'players': [1]
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

    def test_no_records_with_no_players_config(self, target):
        expected = 2

        tap = TapFPL(config={
            '_stream': 'player-details'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        assert expected == len(target_stdout.getvalue().split('\n'))

class TestStandingsStream:
    def test_raise_error_with_no_league_id_config(self, target):
        tap = TapFPL(config={
            '_stream': 'standings'
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

    @patch('singer_sdk.streams.rest.requests')
    def test_load_standings(self, mock_requests, mock_session, target):
        expected = [{
            'last_updated_data': '2022-01-01T00:00:00Z',
            'league': {
                'id': 1,
                'name': 'League Name',
                'created': '2022-01-01T00:00:00Z',
                'closed': False,
                'max_entries': None,
                'league_type': 'x',
                'scoring': 'c',
                'admin_entry': 1,
                'start_event': 1,
                'code_privacy': 'p',
                'has_cup': False,
                'cup_league': None,
                'rank': None
            },
            'standings': {
                'results': [
                    {
                        'id': 1,
                        'event_total': 1,
                        'player_name': 'John Doe',
                        'rank': 1,
                        'last_rank': 1,
                        'rank_sort': 1,
                        'total': 1,
                        'entry': 1,
                        'entry_name': 'Team Name'
                    }
                ]
            }
        }]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected[0])

        tap = TapFPL(config={
            '_stream': 'standings',
            'league_id': 1
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = {'last_updated_data': 1}
        

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'standings',
            'league_id': 1
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

class TestSelectionsStream:
    def test_get_url_with_manager_id_and_gameweek(self):
        expected = 'https://fantasy.premierleague.com/api/entry/1/event/1/picks'
        
        tap = TapFPL(config={
            '_stream': 'selections'
        })

        assert expected == tap.discover_streams()[0].get_url({}, 1, 1)

    @patch('singer_sdk.streams.rest.requests')
    def test_no_output_with_no_config(self, mock_requests, mock_session, target):
        expected = 2
        
        tap = TapFPL(config={
            '_stream': 'selections'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = len(target_stdout.getvalue().split('\n'))

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_picks_for_one_gameweek_for_one_manager(self, mock_requests, mock_session, target):
        expected = [
            {
                'gameweek': 1,
                'manager_id': 1,
                'active_chip': None,
                'automatic_subs': [
                    {
                        'entry': 1,
                        'element_in': 1,
                        'element_out': 2,
                        'event': 8
                    }
                ],
                'entry_history': {
                    'event': 1,
                    'points': 1,
                    'total_points': 1,
                    'rank': 1,
                    'rank_sort': 1,
                    'overall_rank': 1,
                    'bank': 0,
                    'value': 1000,
                    'event_transfers': 1,
                    'event_transfers_cost': 0,
                    'points_on_bench': 1
                },
                'picks': [
                    {
                        'element': 1,
                        'position': 1,
                        'multiplier': 1,
                        'is_captain': False,
                        'is_vice_captain': False
                    }
                ]
            }
        ]

        response = deepcopy(expected[0])
        del response['gameweek']
        del response['manager_id']

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = response

        tap = TapFPL(config={
            '_stream': 'selections',
            'managers': [1],
            'gameweeks': [1]
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_picks_for_one_gameweek_for_multiple_managers(self, mock_requests, mock_session, target):
        expected = [
            {
                'gameweek': 1,
                'manager_id': 1,
                'active_chip': None,
                'automatic_subs': [
                    {
                        'entry': 1,
                        'element_in': 1,
                        'element_out': 2,
                        'event': 8
                    }
                ],
                'entry_history': {
                    'event': 1,
                    'points': 1,
                    'total_points': 1,
                    'rank': 1,
                    'rank_sort': 1,
                    'overall_rank': 1,
                    'bank': 0,
                    'value': 1000,
                    'event_transfers': 1,
                    'event_transfers_cost': 0,
                    'points_on_bench': 1
                },
                'picks': [
                    {
                        'element': 1,
                        'position': 1,
                        'multiplier': 1,
                        'is_captain': False,
                        'is_vice_captain': False
                    }
                ]
            },
            {
                'gameweek': 1,
                'manager_id': 2,
                'active_chip': None,
                'automatic_subs': [
                    {
                        'entry': 1,
                        'element_in': 1,
                        'element_out': 2,
                        'event': 8
                    }
                ],
                'entry_history': {
                    'event': 1,
                    'points': 1,
                    'total_points': 1,
                    'rank': 1,
                    'rank_sort': 1,
                    'overall_rank': 1,
                    'bank': 0,
                    'value': 1000,
                    'event_transfers': 1,
                    'event_transfers_cost': 0,
                    'points_on_bench': 1
                },
                'picks': [
                    {
                        'element': 1,
                        'position': 1,
                        'multiplier': 1,
                        'is_captain': False,
                        'is_vice_captain': False
                    }
                ]
            }
        ]

        response = deepcopy(expected)
        for item in response:
            del item['manager_id']
            del item['gameweek']

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.side_effect = response

        tap = TapFPL(config={
            '_stream': 'selections',
            'managers': [1, 2],
            'gameweeks': [1]
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_picks_for_multiple_gameweeks_for_one_manager(self, mock_requests, mock_session, target):
        expected = [
            {
                'gameweek': 1,
                'manager_id': 1,
                'active_chip': None,
                'automatic_subs': [
                    {
                        'entry': 1,
                        'element_in': 1,
                        'element_out': 2,
                        'event': 8
                    }
                ],
                'entry_history': {
                    'event': 1,
                    'points': 1,
                    'total_points': 1,
                    'rank': 1,
                    'rank_sort': 1,
                    'overall_rank': 1,
                    'bank': 0,
                    'value': 1000,
                    'event_transfers': 1,
                    'event_transfers_cost': 0,
                    'points_on_bench': 1
                },
                'picks': [
                    {
                        'element': 1,
                        'position': 1,
                        'multiplier': 1,
                        'is_captain': False,
                        'is_vice_captain': False
                    }
                ]
            },
            {
                'gameweek': 2,
                'manager_id': 1,
                'active_chip': None,
                'automatic_subs': [
                    {
                        'entry': 1,
                        'element_in': 1,
                        'element_out': 2,
                        'event': 8
                    }
                ],
                'entry_history': {
                    'event': 1,
                    'points': 1,
                    'total_points': 1,
                    'rank': 1,
                    'rank_sort': 1,
                    'overall_rank': 1,
                    'bank': 0,
                    'value': 1000,
                    'event_transfers': 1,
                    'event_transfers_cost': 0,
                    'points_on_bench': 1
                },
                'picks': [
                    {
                        'element': 1,
                        'position': 1,
                        'multiplier': 1,
                        'is_captain': False,
                        'is_vice_captain': False
                    }
                ]
            }
        ]

        response = deepcopy(expected)
        for item in response:
            del item['manager_id']
            del item['gameweek']

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.side_effect = response

        tap = TapFPL(config={
            '_stream': 'selections',
            'managers': [1],
            'gameweeks': [1, 2]
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_raise_error_on_inaccurate_schema(self, mock_requests, mock_session, target):
        expected = {'active_chip': 0}
        

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = deepcopy(expected)

        tap = TapFPL(config={
            '_stream': 'selections',
            'managers': [1],
            'gameweeks': [1]
        })

        with raises(Exception):
            tap_to_target_sync_test(tap, target)

    
        



    


