from turtle import home
from black import target_version_option_callback
from pytest import fixture, raises
from tap_statbunker.tap import TapStatbunker
from singer_sdk.testing import tap_to_target_sync_test
from target_tester.target import TargetTester
from http.server import HTTPServer

@fixture
def target():
    return TargetTester()

class TestSeasonsStream:

    def test_extract_single_season(self, httpserver, target):
        response = """
        <form id="comp">
        <select name="comp_id">
        <option value="-1">Select competition</option>
        <option value="689">Premier League 21/22</option>
        </select>
        </form>
        """

        expected = [{
            'id': 689,
            'name': 'Premier League 21/22',
            'years': {
                'start': 2021,
                'end': 2022
            }
        }]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'seasons',
            'comp_id': 689
        })
        stream = tap.streams['seasons']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_seasons(self, httpserver, target):
        response = """
        <form id="comp">
        <select name="comp_id">
        <option value="-1">Select competition</option>
        <option value="689">Premier League 21/22</option>
        <option value="600">Premier League 99/00</option>
        </select>
        </form>
        """

        expected = [
            {
                'id': 689,
                'name': 'Premier League 21/22',
                'years': {
                    'start': 2021,
                    'end': 2022
                }
            },
            {
                'id': 600,
                'name': 'Premier League 99/00',
                'years': {
                    'start': 1999,
                    'end': 2000
                }
            }
        ]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'seasons',
            'comp_id': 689
        })
        stream = tap.streams['seasons']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_response_does_not_include_comp_form(self, httpserver, target):
        response = """
        <div></div>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'seasons',
            'comp_id': 689
        })
        stream = tap.streams['seasons']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find season form in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_less_than_2_options_in_comp_dropdown(self, httpserver, target):
        response = """
        <form id="comp">
        <select name="comp_id">
        <option value="-1">Select competition</option>
        </select>
        </form>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'seasons',
            'comp_id': 689
        })
        stream = tap.streams['seasons']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find season dropdown options in page source'):
            tap_to_target_sync_test(tap, target)
    
    def test_raise_error_when_no_comp_id_config(self, httpserver, target):
        response = """
        <form id="comp">
        <select name="comp_id">
        <option value="-1">Select competition</option>
        <option value="689">Premier League 21/22</option>
        <option value="600">Premier League 99/00</option>
        </select>
        </form>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'seasons',
        })
        stream = tap.streams['seasons']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='comp_id is required'):
            tap_to_target_sync_test(tap, target)

class TestMatchesStream:
    def test_extract_single_match_for_single_date(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=5"></a>
        <p>Arsenal</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=4"></a>
        <p>Liverpool</p>
        </span>
        <span class="matchTime"><p>20:15</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        </div>
        """
        expected = [
            {
                'id': 115718,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 5,
                        'name': 'Arsenal'
                    },
                    'away': {
                        'id': 4,
                        'name': 'Liverpool'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T20:15:00'
            }
        ]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_matches_for_single_date(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=5"></a>
        <p>Arsenal</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=4"></a>
        <p>Liverpool</p>
        </span>
        <span class="matchTime"><p>20:15</p></span>
        <span class="matchStatLink"></span>
        </li>
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=749"></a>
        <p>Brighton & Hove Albion</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Brighton---Hove-Albion-VS-Tottenham-Hotspur?comp_id=689&match_id=115717&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=19"></a>
        <p>Tottenham Hotspur</p>
        </span>
        <span class="matchTime"><p>19:30</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        </div>
        """
        expected = [
            {
                'id': 115718,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 5,
                        'name': 'Arsenal'
                    },
                    'away': {
                        'id': 4,
                        'name': 'Liverpool'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T20:15:00'
            },
            {
                'id': 115717,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 749,
                        'name': 'Brighton & Hove Albion'
                    },
                    'away': {
                        'id': 19,
                        'name': 'Tottenham Hotspur'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T19:30:00'
            }
        ]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_matches_for_multiple_dates(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=5"></a>
        <p>Arsenal</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=4"></a>
        <p>Liverpool</p>
        </span>
        <span class="matchTime"><p>20:15</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Monday 14 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=749"></a>
        <p>Brighton & Hove Albion</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Brighton---Hove-Albion-VS-Tottenham-Hotspur?comp_id=689&match_id=115717&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=19"></a>
        <p>Tottenham Hotspur</p>
        </span>
        <span class="matchTime"><p>19:30</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        </div>
        """
        expected = [
            {
                'id': 115718,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 5,
                        'name': 'Arsenal'
                    },
                    'away': {
                        'id': 4,
                        'name': 'Liverpool'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T20:15:00'
            },
            {
                'id': 115717,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 749,
                        'name': 'Brighton & Hove Albion'
                    },
                    'away': {
                        'id': 19,
                        'name': 'Tottenham Hotspur'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-14T19:30:00'
            }
        ]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_matches_from_multiple_pages(self, httpserver, target):
        response_1 = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=5"></a>
        <p>Arsenal</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=4"></a>
        <p>Liverpool</p>
        </span>
        <span class="matchTime"><p>20:15</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        <div class="pagination">
        <ul>
        <li class="pagArrow">
        <a href="/1"><p>&#155;</p></a>
        </li>
        <li class="pagArrow">
        <a><p>&#155;&#155;</p></a>
        </li>
        </ul>
        </div>
        </div>
        """

        response_2 = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Monday 14 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=749"></a>
        <p>Brighton & Hove Albion</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Brighton---Hove-Albion-VS-Tottenham-Hotspur?comp_id=689&match_id=115717&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=19"></a>
        <p>Tottenham Hotspur</p>
        </span>
        <span class="matchTime"><p>19:30</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        <div class="pagination">
        </div>
        </div>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response_1, content_type='text/html')

        httpserver.expect_request('/1').respond_with_data(response_2, content_type='text/html')

        expected = [
            {
                'id': 115718,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 5,
                        'name': 'Arsenal'
                    },
                    'away': {
                        'id': 4,
                        'name': 'Liverpool'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T20:15:00'
            },
            {
                'id': 115717,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 749,
                        'name': 'Brighton & Hove Albion'
                    },
                    'away': {
                        'id': 19,
                        'name': 'Tottenham Hotspur'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-14T19:30:00'
            }
        ]

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual
    
    def test_extract_single_match_for_specific_team(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span class="matchDropdown"></span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=5"></a>
        <p>Arsenal</p>
        </span>
        <span class="matchVs">
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        <p>0 - 2</p>
        </a>
        </span>
        <span class="matchTeam">
        <a></a>
        <a href="/competitions/LastMatches?comp_id=689&club_id=4"></a>
        <p>Liverpool</p>
        </span>
        <span class="matchTime"><p>20:15</p></span>
        <span class="matchStatLink"></span>
        </li>
        </ul>
        </div>
        """
        expected = [
            {
                'id': 115718,
                'comp_id': 689,
                'teams': {
                    'home': {
                        'id': 5,
                        'name': 'Arsenal'
                    },
                    'away': {
                        'id': 4,
                        'name': 'Liverpool'
                    }
                },
                'scores': {
                    'home': 0,
                    'away': 2
                },
                'timestamp': '2022-03-16T20:15:00'
            }
        ]

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'club_id': 5,
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_spans_in_li_is_not_6(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li></li>
        </ul>
        </div>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Structure of results row is incorrect. It must have 6 spans'):
            tap_to_target_sync_test(tap, target)
    
    def test_raise_error_when_more_than_one_link_in_vs_span(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span></span>
        <span></span>
        <span>
        <a></a>
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-2022">
        </span>
        <span></span>
        <span></span>
        <span></span>
        </li>
        </ul>
        </div>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Unable to fetch id and comp_id because there are more than one links in vs span'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_fixture_link_is_incorrect(self, httpserver, target):
        response = """
        <div class="upcomingMatchesCon">
        <div class="upcomingMatchesTitle">
        <h1>Latest results</h1>
        </div>
        <div class="upcomingMatchesTitle">
        <h2>Premier League 21/22</h2>
        </div>
        <div class="upcomingMatchesTitle slateGrey">
        <h3>Wednesday 16 March 2022</h3>
        </div>
        <ul class="matchLineup">
        <li>
        <span></span>
        <span></span>
        <span>
        <a href="https://www.statbunker.com/competitions/MatchDetails/Premier-League-21/22/Arsenal-VS-Liverpool?comp_id=689&match_id=115718&date=16-Mar-">
        </span>
        <span></span>
        <span></span>
        <span></span>
        </li>
        </ul>
        </div>
        """

        httpserver.expect_request('/competitions/LastMatches').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'matches',
            'comp_id': 689
        })
        stream = tap.streams['matches']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Unable to fetch id and comp_id because parse vs span link'):
            tap_to_target_sync_test(tap, target)

class TestMatchDetailsStream:
    def test_get_path(self):
        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']

        expected = 'competitions/MatchDetails/Premier-League-21/22'

        actual = stream.path

        assert expected == actual

    def test_extract_single_goal_for_single_team(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div class="matchReportTitle">
        <div class="titleIntLeft"><h2>Wolverhampton Wanderers</h2></div>
        <div class="titleIntCenter"><h2>2-3</h2></div>
        <div class="titleIntRight"><h2>Leeds United</h2></div>
        </div>
        <div id="matchStats"></div>
        <div class="matchReportInt">
        <div class="matchReportSubInt">
        <p>26` Jonny => Francisco Trincao</p>
        <small>Right Foot, Left, Open Play, 18 yrds </small>
        </div>
        </div>
        <div class="matchReportInt"></div>
        <ul class="matchReportInt"></ul>
        <ul class="matchReportInt"></ul>
        </div>
        """

        expected = [
            {
                'id': 115720,
                'comp_id': 689,
                'teams': {
                    'home': 'Wolverhampton Wanderers',
                    'away': 'Leeds United'
                },
                'goals': {
                    'home': [
                        {
                            'minute': 26,
                            'scorer': 'Francisco Trincao',
                            'assister': 'Jonny',
                            'foot': 'Right Foot',
                            'shot_direction': 'left',
                            'type': 'Open Play',
                            'distance': 18
                        }
                    ],
                    'away': []
                }
            }
        ]
        
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_single_goal_for_each_team(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div class="matchReportTitle">
        <div class="titleIntLeft"><h2>Wolverhampton Wanderers</h2></div>
        <div class="titleIntCenter"><h2>2-3</h2></div>
        <div class="titleIntRight"><h2>Leeds United</h2></div>
        </div>
        <div id="matchStats"></div>
        <div class="matchReportInt">
        <div class="matchReportSubInt">
        <p>26` Jonny => Francisco Trincao</p>
        <small>Right Foot, Left, Open Play, 18 yrds </small>
        </div>
        </div>
        <div class="matchReportInt">
        <div class="matchReportSubInt">
        <p>63` Jack Harrison</p>
        <small>Right Foot, Centre, Open Play, 18 yrds </small>
        </div>
        </div>
        <ul class="matchReportInt"></ul>
        <ul class="matchReportInt"></ul>
        </div>
        """

        expected = [
            {
                'id': 115720,
                'comp_id': 689,
                'teams': {
                    'home': 'Wolverhampton Wanderers',
                    'away': 'Leeds United'
                },
                'goals': {
                    'home': [
                        {
                            'minute': 26,
                            'scorer': 'Francisco Trincao',
                            'assister': 'Jonny',
                            'foot': 'Right Foot',
                            'shot_direction': 'left',
                            'type': 'Open Play',
                            'distance': 18
                        }
                    ],
                    'away': [
                        {
                            'minute': 63,
                            'scorer': 'Jack Harrison',
                            'assister': None,
                            'foot': 'Right Foot',
                            'shot_direction': 'centre',
                            'type': 'Open Play',
                            'distance': 18
                        }
                    ]
                }
            }
        ]
        
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_no_goals_for_each_team(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div class="matchReportTitle">
        <div class="titleIntLeft"><h2>Wolverhampton Wanderers</h2></div>
        <div class="titleIntCenter"><h2>2-3</h2></div>
        <div class="titleIntRight"><h2>Leeds United</h2></div>
        </div>
        <div id="matchStats"></div>
        <div class="matchReportInt">
        </div>
        <div class="matchReportInt">
        </div>
        <ul class="matchReportInt"></ul>
        <ul class="matchReportInt"></ul>
        </div>
        """

        expected = [
            {
                'id': 115720,
                'comp_id': 689,
                'teams': {
                    'home': 'Wolverhampton Wanderers',
                    'away': 'Leeds United'
                },
                'goals': {
                    'home': [],
                    'away': []
                }
            }
        ]
        
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_goals_for_each_team(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div class="matchReportTitle">
        <div class="titleIntLeft"><h2>Wolverhampton Wanderers</h2></div>
        <div class="titleIntCenter"><h2>2-3</h2></div>
        <div class="titleIntRight"><h2>Leeds United</h2></div>
        </div>
        <div id="matchStats"></div>
        <div class="matchReportInt">
        <div class="matchReportSubInt">
        <p>26` Jonny => Francisco Trincao</p>
        <small>Right Foot, Left, Open Play, 18 yrds </small>
        </div>
        <div class="matchReportSubInt">
        <p>45` Francisco Trincao => Daniel Podence</p>
        <small>Left Foot, Left, Free Kick, 18 yrds </small>
        </div>
        </div>
        <div class="matchReportInt">
        <div class="matchReportSubInt">
        <p>63` Jack Harrison</p>
        <small>Right Foot, Centre, Open Play, 18 yrds </small>
        </div>
        <div class="matchReportSubInt">
        <p>66` Rodrigo Moreno => Sam Greenwood</p>
        <small>Left Foot, Right, Open Play, 6 yrds </small>
        </div>
        </div>
        <ul class="matchReportInt"></ul>
        <ul class="matchReportInt"></ul>
        </div>
        """

        expected = [
            {
                'id': 115720,
                'comp_id': 689,
                'teams': {
                    'home': 'Wolverhampton Wanderers',
                    'away': 'Leeds United'
                },
                'goals': {
                    'home': [
                        {
                            'minute': 26,
                            'scorer': 'Francisco Trincao',
                            'assister': 'Jonny',
                            'foot': 'Right Foot',
                            'shot_direction': 'left',
                            'type': 'Open Play',
                            'distance': 18
                        },
                        {
                            'minute': 45,
                            'scorer': 'Daniel Podence',
                            'assister': 'Francisco Trincao',
                            'foot': 'Left Foot',
                            'shot_direction': 'left',
                            'type': 'Free Kick',
                            'distance': 18
                        }
                    ],
                    'away': [
                        {
                            'minute': 63,
                            'scorer': 'Jack Harrison',
                            'assister': None,
                            'foot': 'Right Foot',
                            'shot_direction': 'centre',
                            'type': 'Open Play',
                            'distance': 18
                        },
                        {
                            'minute': 66,
                            'scorer': 'Sam Greenwood',
                            'assister': 'Rodrigo Moreno',
                            'foot': 'Left Foot',
                            'shot_direction': 'right',
                            'type': 'Open Play',
                            'distance': 6
                        }
                    ]
                }
            }
        ]
        
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_title_bar_is_not_found(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div>
        """
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot fetch team names because title bar is not present in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_stats_containers_are_not_complete(self, httpserver, target):
        response = """
        <div id="matchReportCon">
        <ul class="matchReportInt"></ul>
        <div class="matchReportTitle">
        <div class="titleIntLeft"><h2>Wolverhampton Wanderers</h2></div>
        <div class="titleIntCenter"><h2>2-3</h2></div>
        <div class="titleIntRight"><h2>Leeds United</h2></div>
        <div id="matchStats"></div>
        <div>
        """
        httpserver.expect_request('/competitions/MatchDetails/Premier-League-21/22').respond_with_data(response, content_type='text/html')

        tap = TapStatbunker(config={
            '_stream': 'match_details',
            'comp_id': 689,
            'match_id': 115720,
            'comp_name': 'Premier League 21/22'
        })
        stream = tap.streams['match_details']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot extract goals information because stats containers is not complete in page source'):
            tap_to_target_sync_test(tap, target)
