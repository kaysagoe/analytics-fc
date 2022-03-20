from pytest import fixture, raises
from tap_sofifa.tap import TapSoFIFA
from singer_sdk.testing import tap_to_target_sync_test
from target_tester.target import TargetTester
from http.server import HTTPServer

@fixture
def target():
    return TargetTester()


class TestVersionsStream:
    
    def test_extract_single_version(self, httpserver: HTTPServer, target):
        expected = [
            {
                'name': 'FIFA 22',
                'r': '100000',
                'set': 'true'
            }
        ]

        response = """
        <html>
        <body>
        <a>Continue to Site</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a href="/r=100000&set=true">FIFA 22</a>
        </div>
        </body>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'versions'
        })
        stream = tap.streams['versions']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual
    
    def test_extract_multiple_versions(self, httpserver, target):
        expected = [
            {
                'name': 'FIFA 22',
                'r': '100000',
                'set': 'true'
            },
            {
                'name': 'FIFA 21',
                'r': '100001',
                'set': 'true'
            }
        ]

        response = """
        <html>
        <body>
        <a>Continue to Site</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a href="/r=100000&set=true">FIFA 22</a>
        <a href="/r=100001&set=true">FIFA 21</a>
        </div>
        </body>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'versions'
        })
        stream = tap.streams['versions']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_less_than_2_elements_with_bp3_menu_class(self, httpserver):
        response = """
        <html>
        <body>
        <a>Continue to Site</a>
        <div class="bp3-menu"></div>
        </body>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'versions'
        })
        stream = tap.streams['versions']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find FIFA versions menu in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_less_than_no_options_in_dropdown(self, httpserver):
        response = """
        <html>
        <body>
        <a>Continue to Site</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        </body>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'versions'
        })
        stream = tap.streams['versions']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Fifa versions menu dropdown contains no options'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_dropdown_options_do_not_contain_fifa(self, httpserver):
        response = """
        <html>
        <body>
        <a>Continue to Site</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a>FM 22</a>
        </div>
        </body>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'versions'
        })
        stream = tap.streams['versions']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Wrong menu in page source selected'):
            tap_to_target_sync_test(tap, target)
        
class TestChangesStream:
    def test_extract_single_change(self, httpserver, target):
        expected = [
            {
                'name': 'Feb 22, 2022',
                'timestamp': '2022-02-22T00:00:00',
                'r': '200000',
                'set': 'true'
            }
        ]

        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a href="/?r=200000&set=true">Feb 22, 2022</a>
        </div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 22
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_extract_multiple_changes(self, httpserver, target):
        expected = [
            {
                'name': 'Feb 22, 2022',
                'timestamp': '2022-02-22T00:00:00',
                'r': '200000',
                'set': 'true'
            },
            {
                'name': 'Feb 18, 2022',
                'timestamp': '2022-02-18T00:00:00',
                'r': '200001',
                'set': 'true'
            }
        ]

        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a href="/?r=200000&set=true">Feb 22, 2022</a>
        <a href="/?r=200001&set=true">Feb 18, 2022</a>
        </div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 22
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_with_invalid_game_year(self, httpserver, target):
        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a href="/?r=200000&set=true">Feb 22, 2022</a>
        <a href="/?r=200001&set=true">Feb 18, 2022</a>
        </div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 23
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='23 does not exist in SoFIFA database'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_cannot_find_dropdown(self, httpserver):
        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 22
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find changes menu in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_with_empty_dropdown(self, httpserver):
        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 22
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Changes menu dropdown contains no options'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_on_wrong_dropdown_selected(self, httpserver):
        response = """
        <html>
        <a>Continue to Site</a>
        <a>FIFA 22</a>
        <a>FIFA 22</a>
        <div class="bp3-menu"></div>
        <div class="bp3-menu"></div>
        <div class="bp3-menu">
        <a>FIFA 22</a>
        </div>
        </html>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'changes',
            'game_year': 22
        })
        stream = tap.streams['changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Wrong menu in page source selected'):
            tap_to_target_sync_test(tap, target)

class TestPlayerChangesStream:
    def test_get_url_params(self):
        expected = {
            'type': 'all',
            'set': 'true',
            'lg%5B0%5D': '1',
            'r': '200000'
        }

        tap = TapSoFIFA(config={
            '_stream': 'player_changes',
            'league_id': 1,
            'change_id': 200000
        })

        assert expected == tap.streams['player_changes'].get_url_params({})

    def test_extract_single_player_change_for_permanent_contract(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <table>
        <tbody>
        <tr>
        <td><figure></figure></td>
        <td>
        <a href="/player/100000/john-doe/200000" aria-label="John Doe">
        <div>J. Doe</div>
        </a>
        <img title="Nigeria" />
        <a><span>RW</span></a>
        <a><span>ST</span></a>
        </td>
        <td>23</td>
        <td><span>99</span></td>
        <td><span>99</span></td>
        <td>
        <div>
        <figure><img /></figure>
        <a href="/team/1/manchester-united">Manchester United</a>
        <div>2021 ~ 2023</div>
        </div>
        </td>
        <td>€50M</td>
        <td>€100K</td>
        <td><span>1000</span></td>
        </tr>
        </tbody>
        </table>
        """

        expected = [
            {
                'id': 100000,
                'change_id': 200000,
                'name': 'John Doe',
                'nationality': 'Nigeria',
                'positions': ['RW', 'ST'],
                'age': 23,
                'overall_rating': 99,
                'potential_rating': 99,
                'team': {
                    'id': 1,
                    'name': 'Manchester United'
                },
                'contract': {
                    'on_loan': False,
                    'year_start': 2021,
                    'year_end': 2023
                },
                'value': 'â‚¬50M',
                'wage': 'â‚¬100K',
                'total': 1000
            }
        ]

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])
        print(expected)
        print(actual)

        assert expected == actual

    def test_extract_single_player_change_for_loan(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <table>
        <tbody>
        <tr>
        <td><figure></figure></td>
        <td>
        <a href="/player/100000/john-doe/200000" aria-label="John Doe">
        <div>J. Doe</div>
        </a>
        <img title="Nigeria" />
        <a><span>RW</span></a>
        <a><span>ST</span></a>
        </td>
        <td>23</td>
        <td><span>99</span></td>
        <td><span>99</span></td>
        <td>
        <div>
        <figure><img /></figure>
        <a href="/team/1/manchester-united">Manchester United</a>
        <div>Jun 30, 2022<span>On Loan</span></div>
        </div>
        </td>
        <td>€50M</td>
        <td>€100K</td>
        <td><span>1000</span></td>
        </tr>
        </tbody>
        </table>
        """

        expected = [
            {
                'id': 100000,
                'change_id': 200000,
                'name': 'John Doe',
                'nationality': 'Nigeria',
                'positions': ['RW', 'ST'],
                'age': 23,
                'overall_rating': 99,
                'potential_rating': 99,
                'team': {
                    'id': 1,
                    'name': 'Manchester United'
                },
                'contract': {
                    'on_loan': True,
                    'year_start': None,
                    'year_end': 2022
                },
                'value': 'â‚¬50M',
                'wage': 'â‚¬100K',
                'total': 1000
            }
        ]

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])
        print(expected)
        print(actual)

        assert expected == actual

    def test_extract_players_from_multiple_pages(self, httpserver, target):
        response_p1 = """
        <a>Continue to Site</a>
        <table>
        <tbody>
        <tr>
        <td><figure></figure></td>
        <td>
        <a href="/player/100000/john-doe/200000" aria-label="John Doe">
        <div>J. Doe</div>
        </a>
        <img title="Nigeria" />
        <a><span>RW</span></a>
        <a><span>ST</span></a>
        </td>
        <td>23</td>
        <td><span>99</span></td>
        <td><span>99</span></td>
        <td>
        <div>
        <figure><img /></figure>
        <a href="/team/1/manchester-united">Manchester United</a>
        <div>Jun 30, 2022<span>On Loan</span></div>
        </div>
        </td>
        <td>€50M</td>
        <td>€100K</td>
        <td><span>1000</span></td>
        </tr>
        </tbody>
        </table>
        <a href="/1"><span>NEXT</span></a>
        """

        response_p2 = """
        <table>
        <tbody>
        <tr>
        <td><figure></figure></td>
        <td>
        <a href="/player/100001/john-doe/200000" aria-label="Jane Doe">
        <div>J. Doe</div>
        </a>
        <img title="Nigeria" />
        <a><span>RW</span></a>
        <a><span>ST</span></a>
        </td>
        <td>23</td>
        <td><span>99</span></td>
        <td><span>99</span></td>
        <td>
        <div>
        <figure><img /></figure>
        <a href="/team/2/arsenal">Arsenal</a>
        <div>Jun 30, 2022<span>On Loan</span></div>
        </div>
        </td>
        <td>€50M</td>
        <td>€100K</td>
        <td><span>1000</span></td>
        </tr>
        </tbody>
        </table>
        """

        expected = [
            {
                'id': 100000,
                'change_id': 200000,
                'name': 'John Doe',
                'nationality': 'Nigeria',
                'positions': ['RW', 'ST'],
                'age': 23,
                'overall_rating': 99,
                'potential_rating': 99,
                'team': {
                    'id': 1,
                    'name': 'Manchester United'
                },
                'contract': {
                    'on_loan': True,
                    'year_start': None,
                    'year_end': 2022
                },
                'value': 'â‚¬50M',
                'wage': 'â‚¬100K',
                'total': 1000
            },
            {
                'id': 100001,
                'change_id': 200000,
                'name': 'Jane Doe',
                'nationality': 'Nigeria',
                'positions': ['RW', 'ST'],
                'age': 23,
                'overall_rating': 99,
                'potential_rating': 99,
                'team': {
                    'id': 2,
                    'name': 'Arsenal'
                },
                'contract': {
                    'on_loan': True,
                    'year_start': None,
                    'year_end': 2022
                },
                'value': 'â‚¬50M',
                'wage': 'â‚¬100K',
                'total': 1000
            }
        ]

        httpserver.expect_request('/').respond_with_data(response_p1, content_type='text/html')
        httpserver.expect_request('/1').respond_with_data(response_p2, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_tbody_is_not_found(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='SoFIFA data not available'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_no_row_is_found(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <table>
        <tbody></tbody>
        </table>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='SoFIFA data not available'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_columns_in_table_are_not_9(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <table>
        <tbody>
        <tr>
        <td></td>
        </tr>
        </tbody>
        </table>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Incorrect data format'):
            tap_to_target_sync_test(tap, target)

    def test_incorrect_tags_in_column(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <table>
        <tbody>
        <tr>
        <td></td>
        <td>
        <a href="/player/100001/john-doe/200000" aria-label="Jane Doe">
        <div>J. Doe</div>
        </a>
        <img title="Nigeria" />
        <a><span>RW</span></a>
        <a><span>ST</span></a>
        </td>
        <td>23</td>
        <td><span>99</span></td>
        <td><span>99</span></td>
        <td>
        <div>
        <figure><img /></figure>
        <a href="/team/2/arsenal">Arsenal</a>
        <div>Jun 30, 2022<span>On Loan</span></div>
        </div>
        </td>
        <td>€50M</td>
        <td>€100K</td>
        <td><span>1000</span></td>
        </tr>
        </tbody>
        </table>
        """

        httpserver.expect_request('/').respond_with_data(response, content_type='text/html')


        tap = TapSoFIFA(config={
            '_stream': 'player_changes'
        })
        stream = tap.streams['player_changes']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Incorrect DOM structure for column 0'):
            tap_to_target_sync_test(tap, target)

class TestPlayerDetailStream:
    def test_get_url_params(self):
        expected = {
            'set': 'true',
            'r': '200000'
        }

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']

        assert expected == stream.get_url_params({})

    def test_get_path(self):
        expected = 'player/100000'

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']

        assert expected == stream.path

    def test_extract_player_details(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        <section>
        <div>
        <div>
        <span>79</span>
        <div>Overall Rating</div>
        </div>
        </div>
        <div>
        <div>
        <span>84</span>
        <div>Potential Rating</div>
        </div>
        </div>
        </section>
        <div class="col-12"></div>
        <div class="col-12">
        <div class="block-quarter">
        <div>
        <h5>Attacking</h5>
        <ul>
        <li><span>40</span><span>Crossing</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Skill</h5>
        <ul>
        <li><span>40</span><span>Dribbling</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Movement</h5>
        <ul>
        <li><span>40</span><span>Acceleration</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Power</h5>
        <ul>
        <li><span>40</span><span>Shot Power</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Mentality</h5>
        <ul>
        <li><span>40</span><span>Aggression</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Defending</h5>
        <ul>
        <li><span>40</span><span>Defensive Awareness</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter">
        <div>
        <h5>Goalkeeping</h5>
        <ul>
        <li><span>40</span><span>GK Diving</span></li>
        </ul>
        </div>
        </div>
        <div class="block-quarter"></div>
        </div>
        """

        expected = [{
            'id': 100000,
            'change_id': 200000,
            'name': 'John Doe',
            'overall_rating': 79,
            'potential_rating': 84,
            'attacking': {
                'crossing': 40
            },
            'skill': {
                'dribbling': 40
            },
            'movement': {
                'acceleration': 40
            },
            'power': {
                'shot_power': 40
            },
            'mentality': {
                'aggression': 40
            },
            'defending': {
                'defensive_awareness': 40
            },
            'goalkeeping': {
                'gk_diving': 40
            }
        }]

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        _, _, target_stdout, _ = tap_to_target_sync_test(tap, target)

        actual = eval(target_stdout.getvalue().split('\n')[0])

        assert expected == actual

    def test_raise_error_when_response_does_not_contain_name_container(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find name container in page source'):
            tap_to_target_sync_test(tap, target)
    
    def test_raise_error_when_response_does_not_include_h1(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"></div>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find name in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_response_does_not_include_average_ratings_section(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find average ratings section in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_response_does_not_2_spans_in_average_ratings_section(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        <section></section>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find average ratings in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_response_has_less_than_2_col_12_divs(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        <section>
        <div>
        <div>
        <span>79</span>
        <div>Overall Rating</div>
        </div>
        </div>
        <div>
        <div>
        <span>84</span>
        <div>Potential Rating</div>
        </div>
        </div>
        </section>
        <div class="col-12"></div>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find container which contains in-depth ratings in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_response_does_not_2_spans_in_average_ratings_section(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        <section></section>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find average ratings in page source'):
            tap_to_target_sync_test(tap, target)

    def test_raise_error_when_response_does_not_have_enough_block_quarter_divs(self, httpserver, target):
        response = """
        <a>Continue to Site</a>
        <div class="info"><h1>John Doe</h1></div>
        <section>
        <div>
        <div>
        <span>79</span>
        <div>Overall Rating</div>
        </div>
        </div>
        <div>
        <div>
        <span>84</span>
        <div>Potential Rating</div>
        </div>
        </div>
        </section>
        <div class="col-12"></div>
        <div class="col-12"></div>
        """

        httpserver.expect_request('/player/100000').respond_with_data(response, content_type='text/html')

        tap = TapSoFIFA(config={
            '_stream': 'player_detail',
            'player_id': 100000,
            'change_id': 200000
        })
        stream = tap.streams['player_detail']
        stream.url_base = httpserver.url_for('/')

        with raises(Exception, match='Cannot find sub-rating block quarters in page source'):
            tap_to_target_sync_test(tap, target)





