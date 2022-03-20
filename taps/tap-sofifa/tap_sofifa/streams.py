"""Stream type classes for tap-sofifa."""

import logging
import re
import sys

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sofifa.client import SoFIFAStream
from bs4 import BeautifulSoup
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from selenium.webdriver.common.by import By

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class VersionsStream(SoFIFAStream):
    """Define custom stream."""
    name = "versions"
    path = ''
    schema_filepath = SCHEMAS_DIR / "versions.json"
    
    def validate_response(self, response: BeautifulSoup) -> None:
        menus = response.find_all(class_ = 'bp3-menu')
        if len(menus) < 2:
            raise RetriableAPIError('Cannot find FIFA versions menu in page source')
        
        links = menus[1].find_all('a')
        if len(links) == 0:
            raise RetriableAPIError('Fifa versions menu dropdown contains no options')
        
        if 'FIFA' not in links[0].get_text():
            raise RetriableAPIError('Wrong menu in page source selected')
        
    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        menu = response.find_all(class_ = 'bp3-menu')
        links = menu[1].find_all('a')
        for link in links:
            yield {
                'name': link.get_text(),
                'r': re.findall(r'\d+', link['href'])[0],
                'set': re.findall(r'set=\D*', link['href'])[0].split('=')[1]
            }

class ChangesStream(SoFIFAStream):
    name = 'changes'
    path = ''
    schema_filepath = SCHEMAS_DIR / "changes.json"

    def validate_response(self, response: BeautifulSoup) -> None:
        menus = response.find_all(class_ = 'bp3-menu')
        if len(menus) < 3:
            raise RetriableAPIError('Cannot find changes menu in page source')
        
        links = menus[2].find_all('a')
        if len(links) == 0:
            raise RetriableAPIError('Changes menu dropdown contains no options')
        
        try:
            datetime.strptime(links[0].get_text(), '%b %d, %Y')
        except:
            raise RetriableAPIError('Wrong menu in page source selected')

    
    def _request(
        self, url: str, context: Optional[dict]
    ) -> BeautifulSoup:
        self.driver.set_page_load_timeout(self.timeout)
        self.driver.get(url)
        
        self.driver.find_element(by=By.PARTIAL_LINK_TEXT, value='FIFA').click()
        
        game_name = ''.join(['FIFA ', str(self.config['game_year'])])
        try:
            self.driver.find_element(by=By.LINK_TEXT, value=game_name).click()
        except NoSuchElementException:
            raise FatalAPIError(f'{game_name} does not exist in SoFIFA database')

        response = BeautifulSoup(self.driver.page_source, 'html.parser')
        self.validate_response(response)
        logging.debug("Response received successfully.")
        return response
    
    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        menu = response.find_all(class_ = 'bp3-menu')
        links = menu[2].find_all('a')
        for link in links:
            yield {
                'name': link.get_text(),
                'timestamp': datetime.strptime(link.get_text(), '%b %d, %Y').isoformat(),
                'r': re.findall(r'\d+', link['href'])[0],
                'set': re.findall(r'set=\D*', link['href'])[0].split('=')[1]
            }

class PlayerChangesStream(SoFIFAStream):
    name = 'player_changes'
    path = ''
    schema_filepath = SCHEMAS_DIR / "player_changes.json"

    def get_url_params(self, context: Optional[dict]):
        params = {
            'type': 'all',
            'set': 'true'   
        }

        if 'league_id' in self.config:
            params['lg%5B0%5D'] = str(self.config['league_id'])

        if 'change_id' in self.config:
            params['r'] = str(self.config['change_id'])
        
        return params

    def validate_response(self, response: BeautifulSoup) -> None:
        tbody = response.find('tbody')
        if not tbody:
            raise RetriableAPIError('SoFIFA data not available')

        first_row = tbody.find('tr')
        if not first_row:
            raise RetriableAPIError('SoFIFA data not available')
        
        columns = first_row.find_all('td')
        if len(columns) != 9:
            raise RetriableAPIError('Incorrect data format')
        
        tags = [
            ['figure'],
            ['a', 'img'],
            [],
            ['span'],
            ['span'],
            ['div'],
            [],
            [],
            ['span']
        ]

        for index, column in enumerate(columns):
            if index == 1:
                continue
            if [tag.name for tag in column if tag.name is not None] != tags[index]:
                raise RetriableAPIError(f'Incorrect DOM structure for column {index}')
        
        if [tag.name for tag in columns[1] if tag.name is not None][:2] != tags[1]:
            raise RetriableAPIError(f'Incorrect DOM structure for column 1')



    def has_next_page(self) -> bool:
        try:
            self.driver.find_element(by=By.LINK_TEXT, value='NEXT')
            return True
        except:
            return False
    
    def go_to_next_page(self) -> None:
        self.driver.find_element(by=By.LINK_TEXT, value='NEXT').click()

    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        tbody = response.find_all('tbody')[0]
        rows = tbody.find_all('tr')

        for row in rows:
            columns = row.find_all('td')
            name_col_tags = columns[1].findChildren()
            team_col_tags = columns[5].findChild().findChildren()
            contract_text = team_col_tags[3].get_text()

            contract_years = None
            on_loan = False
            if '~' in contract_text:
                contract_years = [int(year.strip()) for year in contract_text.split('~')]
            else:
                on_loan = True
                contract_years = [None, int(re.findall(r'\d+', contract_text)[1])]
            yield {
                'id': int(name_col_tags[0]['href'].split('/')[2]),
                'change_id': int(name_col_tags[0]['href'].split('/')[4]),
                'name': name_col_tags[0]['aria-label'],
                'nationality': name_col_tags[2]['title'],
                'positions': [a_tag.get_text() for a_tag in name_col_tags[3:] if a_tag.name == 'a'],
                'age': int(columns[2].get_text()),
                'overall_rating': int(columns[3].get_text()),
                'potential_rating': int(columns[4].get_text()),
                'team': {
                    'id': int(team_col_tags[2]['href'].split('/')[2]),
                    'name': team_col_tags[2].get_text()
                },
                'contract': {
                    'on_loan': on_loan,
                    'year_start': contract_years[0],
                    'year_end': contract_years[1]
                },
                'value': columns[6].get_text(),
                'wage': columns[7].get_text(),
                'total': int(columns[8].get_text())
            }

class PlayerDetailStream(SoFIFAStream):
    name = 'player_detail'
    schema_filepath = SCHEMAS_DIR / "player_detail.json"

    @property
    def path(self):
        return f'player/{self.config["player_id"]}'
    
    def get_url_params(self, context: Optional[dict]):
        params = {
            'set': 'true'
        }
        if 'change_id' in self.config:
            params['r'] = str(self.config['change_id'])
        
        return params
    
    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        result = dict()
        result['id'] = self.config['player_id']
        result['change_id'] = self.config['change_id']
        result['name'] = response.find(class_ = 'info').find('h1').get_text()

        average_ratings = [int(tag.get_text()) for tag in response.find('section').find_all('span')]
        result['overall_rating'] = average_ratings[0]
        result['potential_rating'] = average_ratings[1]
        
        quarters = response.find_all(class_='col-12')[1].find_all(class_='block-quarter')
        for index in range(7):
            quarter = quarters[index]
            quarter_ratings = dict()
            for li in quarter.find_all('li'):
                children = li.findChildren()
                quarter_ratings[children[1].get_text().lower().replace(' ', '_')] = int(children[0].get_text())
            result[quarter.find('h5').get_text().lower()] = quarter_ratings

        yield result

    def validate_response(self, response: BeautifulSoup) -> None:
        if not response.find(class_ = 'info'):
            raise RetriableAPIError('Cannot find name container in page source')

        if not response.find(class_ = 'info').find('h1'):
            raise RetriableAPIError('Cannot find name in page source')
        
        if not response.find('section'):
            raise RetriableAPIError('Cannot find average ratings section in page source')

        if len(response.find('section').find_all('span')) != 2:
            raise RetriableAPIError('Cannot find average ratings in page source')
        
        if len(response.find_all(class_ = 'col-12')) < 2:
            raise RetriableAPIError('Cannot find container which contains in-depth ratings in page source')

        if len(response.find_all(class_='col-12')[1].find_all(class_='block-quarter')) != 8:
            raise RetriableAPIError('Cannot find sub-rating block quarters in page source')







class GroupsStream(SoFIFAStream):
    """Define custom stream."""
    name = "groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()
