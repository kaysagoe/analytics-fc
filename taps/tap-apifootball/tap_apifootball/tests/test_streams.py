from pytest import fixture
from unittest.mock import patch, Mock
from tap_apifootball.tap import TapAPIFootball
from singer_sdk.testing import tap_to_target_sync_test
from target_tester.target import TargetTester
from copy import deepcopy
from datetime import timedelta

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

class TestFixturesStream:
    def test_get_url_params(self):
        expected = {
            'id': 1,
            'live': 'all',
            'date': '2022-01-01',
            'league': 1,
            'season': 2021,
            'team': 1,
            'last': 1,
            'next': 0,
            'from': '2022-01-01',
            'to': '2022-01-01',
            'round': 'round',
            'status': 'status',
            'timezone': 'timezone'
        }

        config = deepcopy(expected)
        config['_stream'] = 'fixtures'
        config['api_key'] = 'key'

        tap = TapAPIFootball(config=config)
        actual = tap.discover_streams()[0].get_url_params({}, {})

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_single_unplayed_fixture(self, mock_requests, mock_session, target):
        expected = [
            {
                'fixture': {
                    'id': 1,
                    'referee': None,
                    'timezone': 'UTC',
                    'date': '2022-01-01T00:00:00+00:00',
                    'timestamp': 10000,
                    'periods': {
                        'first': None,
                        'second': None
                    },
                    'venue': {
                        'id': 1,
                        'name': 'Stadium Name',
                        'city': 'Stadium City'
                    },
                    'status': {
                        'long': 'Not Started',
                        'short': 'NS',
                        'elapsed': None
                    }                    
                },
                'league': {
                    'id': 1,
                    'name': 'League Name',
                    'country': 'League Country',
                    'logo': 'League Logo',
                    'flag': 'Country Flag',
                    'season': 2022,
                    'round': 'Regular Season - 1'
                },
                'teams': {
                    'home': {
                        'id': 1,
                        'name': 'Home Team',
                        'logo': 'Home Team Logo',
                        'winner': None
                    },
                    'away': {
                        'id': 2,
                        'name': 'Away Team',
                        'logo': 'Away Team Logo',
                        'winner': None
                    }
                },
                'goals': {
                    'home': None,
                    'away': None
                },
                'score': {
                    'halftime': {
                        'home': None,
                        'away': None
                    },
                    'fulltime': {
                        'home': None,
                        'away': None,
                    },
                    'extratime': {
                        'home': None,
                        'away': None
                    },
                    'penalty': {
                        'home': None,
                        'away': None
                    }
                }
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config={
            '_stream': 'fixtures',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_single_played_fixture(self, mock_requests, mock_session, target):
        expected = [
            {
                'fixture': {
                    'id': 1,
                    'referee': 'John Doe',
                    'timezone': 'UTC',
                    'date': '2022-01-01T00:00:00+00:00',
                    'timestamp': 10000,
                    'periods': {
                        'first': 10000,
                        'second': 10000
                    },
                    'venue': {
                        'id': 1,
                        'name': 'Stadium Name',
                        'city': 'Stadium City'
                    },
                    'status': {
                        'long': 'Match Finished',
                        'short': 'FT',
                        'elapsed': 90
                    }                    
                },
                'league': {
                    'id': 1,
                    'name': 'League Name',
                    'country': 'League Country',
                    'logo': 'League Logo',
                    'flag': 'Country Flag',
                    'season': 2022,
                    'round': 'Regular Season - 1'
                },
                'teams': {
                    'home': {
                        'id': 1,
                        'name': 'Home Team',
                        'logo': 'Home Team Logo',
                        'winner': False
                    },
                    'away': {
                        'id': 2,
                        'name': 'Away Team',
                        'logo': 'Away Team Logo',
                        'winner': True
                    }
                },
                'goals': {
                    'home': 0,
                    'away': 0
                },
                'score': {
                    'halftime': {
                        'home': 0,
                        'away': 0
                    },
                    'fulltime': {
                        'home': 0,
                        'away': 0,
                    },
                    'extratime': {
                        'home': 0,
                        'away': 0
                    },
                    'penalty': {
                        'home': 4,
                        'away': 5
                    }
                }
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config={
            '_stream': 'fixtures',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])
        assert expected == actual

class TestStatisticsStream:
    def test_get_url_params(self):
        expected = {
            'fixture': 1,
            'team': 1,
            'type': 'Shots on Goal'
        }

        config = deepcopy(expected)
        config['api_key'] = 'api_key'
        config['_stream'] = 'statistics'

        tap = TapAPIFootball(config = config)

        assert expected == tap.discover_streams()[0].get_url_params({}, {})

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_statistics_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'http://logo.com'
                },
                'statistics': [
                    {
                        'type': 'Shots on Goal',
                        'value': 1
                    },
                    {
                        'type': 'Shots off Goal',
                        'value': 0
                    }
                ]
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'statistics',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual
    
    @patch('singer_sdk.streams.rest.requests')
    def test_extract_statistics_for_unplayed_match(self, mock_requests, mock_session, target):
        expected = 2

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': []}

        tap = TapAPIFootball(config= {
            '_stream': 'statistics',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = len(target_stdout.getvalue().split('\n'))

        assert expected == actual

class TestEventsStream:
    def test_get_url_param(self):
        expected = {
            'fixture': 1,
            'team': 1,
            'player': 1,
            'type': 'Shots on goal'
        }

        config = deepcopy(expected)
        config['_stream'] = 'events'
        config['api_key'] = 'key'

        tap = TapAPIFootball(config= config)

        assert expected == tap.discover_streams()[0].get_url_params({}, {})

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_events_with_assists_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'time': {
                    'elapsed': 1,
                    'extra': None
                },
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'Team A Logo'
                },
                'player': {
                    'id': 1,
                    'name': 'Player Name',
                },
                'assist': {
                    'id': 2,
                    'name': 'Assister Name'
                },
                'type': 'Event type',
                'detail': 'Substitution',
                'comments': None
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'events',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual
    
    @patch('singer_sdk.streams.rest.requests')
    def test_extract_events_with_no_assist_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'time': {
                    'elapsed': 1,
                    'extra': None
                },
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'Team A Logo'
                },
                'player': {
                    'id': 1,
                    'name': 'Player Name',
                },
                'assist': {
                    'id': None,
                    'name': None
                },
                'type': 'Event type',
                'detail': 'Substitution',
                'comments': None
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'events',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_events_for_unplayed_match(self, mock_requests, mock_session, target):
        expected = 2

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': []}

        tap = TapAPIFootball(config= {
            '_stream': 'events',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = len(target_stdout.getvalue().split('\n'))

        assert expected == actual

class TestLineupsStream:
    def test_get_url_params(self):
        expected = {
            'fixture': 1,
            'team': 1,
            'player': 1,
            'type': 'Shots on goal'
        }

        config = deepcopy(expected)
        config['_stream'] = 'lineups'
        config['api_key'] = 'key'

        tap = TapAPIFootball(config= config)

        assert expected == tap.discover_streams()[0].get_url_params({}, {})

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_lineups_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'Team A Logo',
                    'colors': {
                        'player': {
                            'primary': 'Primary',
                            'number': 'Number',
                            'border': 'Border'
                        },
                        'goalkeeper': {
                            'primary': 'Primary',
                            'number': 'Number',
                            'border': 'Border'
                        }
                    }
                },
                'coach': {
                    'id': 1,
                    'name': 'Coach Name',
                    'photo': 'Coach Photo'
                },
                'formation': '4-4-2',
                'startXI': [
                    {
                        'player': {
                            'id': 1,
                            'name': 'Player A Name',
                            'number': 1,
                            'pos': 'G',
                            'grid': '1:1'
                        }
                    }
                ],
                'substitutes': [
                    {
                        
                        'player': {
                            'id': 2,
                            'name': 'Player B Name',
                            'number': 2,
                            'pos': 'D',
                            'grid': None
                        }
                        
                    }
                ]
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'lineups',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_lineups_for_unplayed_match(self, mock_requests, mock_session, target):
        expected = 2

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': []}

        tap = TapAPIFootball(config= {
            '_stream': 'lineups',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = len(target_stdout.getvalue().split('\n'))

        assert expected == actual  

class TestPlayerStatistics:
    def test_get_url_params(self):
        expected = {
            'fixture': 1,
            'team': 1,
        }

        config = deepcopy(expected)
        config['_stream'] = 'player_statistics'
        config['api_key'] = 'key'

        tap = TapAPIFootball(config= config)

        assert expected == tap.discover_streams()[0].get_url_params({}, {})
    
    @patch('singer_sdk.streams.rest.requests')
    def test_extract_player_statistics_with_non_nulls_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'Team A Logo',
                    'update': '2022-01-01T00:00:00+00:00'
                },
                'players': [
                    {
                        'player': {
                            'id': 1,
                            'name': 'Player A Name',
                            'photo': 'Player A Photo'
                        },
                        'statistics': [
                            {
                                'games': {
                                    'minutes': 90,
                                    'number': 1,
                                    'position': 'G',
                                    'rating': '5.0',
                                    'captain': False,
                                    'substitute': False
                                },
                                'offsides': 1,
                                'shots': {
                                    'total': 2,
                                    'on': 1
                                },
                                'goals': {
                                    'total': 1,
                                    'conceded': 0,
                                    'assists': 1,
                                    'saves': 1
                                },
                                'passes': {
                                    'total': 1,
                                    'key': 1,
                                    'accuracy': '100'
                                },
                                'tackles': {
                                    'total': 1,
                                    'blocks': 1,
                                    'interceptions': 1
                                },
                                'duels': {
                                    'total': 1,
                                    'won': 1
                                },
                                'dribbles': {
                                    'attempts': 1,
                                    'success': 1,
                                    'past': 1
                                },
                                'fouls': {
                                    'drawn': 1,
                                    'committed': 1
                                },
                                'cards': {
                                    'yellow': 0,
                                    'red': 0
                                },
                                'penalty': {
                                    'won': 1,
                                    'committed': 1,
                                    'scored': 1,
                                    'missed': 1,
                                    'saved': 1
                                }
                            }
                        ]
                    }
                ]
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'player_statistics',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_player_statistics_with_nulls_for_played_match(self, mock_requests, mock_session, target):
        expected = [
            {
                'team': {
                    'id': 1,
                    'name': 'Team A',
                    'logo': 'Team A Logo',
                    'update': '2022-01-01T00:00:00+00:00'
                },
                'players': [
                    {
                        'player': {
                            'id': 1,
                            'name': 'Player A Name',
                            'photo': 'Player A Photo'
                        },
                        'statistics': [
                            {
                                'games': {
                                    'minutes': None,
                                    'number': 1,
                                    'position': 'G',
                                    'rating': None,
                                    'captain': False,
                                    'substitute': False
                                },
                                'offsides': None,
                                'shots': {
                                    'total': None,
                                    'on': None
                                },
                                'goals': {
                                    'total': None,
                                    'conceded': 0,
                                    'assists': None,
                                    'saves': None
                                },
                                'passes': {
                                    'total': None,
                                    'key': None,
                                    'accuracy': None
                                },
                                'tackles': {
                                    'total': None,
                                    'blocks': None,
                                    'interceptions': None
                                },
                                'duels': {
                                    'total': None,
                                    'won': None
                                },
                                'dribbles': {
                                    'attempts': None,
                                    'success': None,
                                    'past': None
                                },
                                'fouls': {
                                    'drawn': None,
                                    'committed': None
                                },
                                'cards': {
                                    'yellow': 0,
                                    'red': 0
                                },
                                'penalty': {
                                    'won': None,
                                    'committed': None,
                                    'scored': 0,
                                    'missed': 0,
                                    'saved': None
                                }
                            }
                        ]
                    }
                ]
            }
        ]

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': deepcopy(expected)}

        tap = TapAPIFootball(config= {
            '_stream': 'player_statistics',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    @patch('singer_sdk.streams.rest.requests')
    def test_extract_player_statistics_for_unplayed_match(self, mock_requests, mock_session, target):
        expected = 2

        mock_requests.Session.return_value = mock_session
        mock_session.send.return_value.json.return_value = {'response': []}

        tap = TapAPIFootball(config= {
            '_stream': 'player_statistics',
            'api_key': 'key'
        })

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)
        actual = len(target_stdout.getvalue().split('\n'))

        assert expected == actual




