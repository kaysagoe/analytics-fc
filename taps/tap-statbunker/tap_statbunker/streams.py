"""Stream type classes for tap-statbunker."""

import re
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from bs4 import BeautifulSoup
from tap_statbunker.client import StatbunkerStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from datetime import datetime
from selenium.webdriver.common.by import By

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")



class SeasonsStream(StatbunkerStream):
    """Define custom stream."""
    name = "seasons"
    path = 'competitions/LastMatches'
    schema_filepath = SCHEMAS_DIR / "seasons.json"

    def get_url_params(self, context: Optional[dict]):
        if 'comp_id' in self.config: 
            return {
                'comp_id': self.config['comp_id']
            }
        else:
            raise Exception('comp_id is required') 
    
    def validate_response(self, response: BeautifulSoup) -> None:
        if not response.find(id = 'comp'):
            raise RetriableAPIError('Cannot find season form in page source')
        
        if len(response.find(id = 'comp').find_all('option')) < 2:
            raise RetriableAPIError('Cannot find season dropdown options in page source')
    
    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        for option in response.find(id = 'comp').find_all('option')[1:]:
            season_years = [int(year) + 1900 if year[0] == '9' else int(year) + 2000 for year in re.findall(r'\d+', option.get_text())]
            yield {
                'id': int(option['value']),
                'name': option.get_text(),
                'years': {
                    'start': season_years[0],
                    'end': season_years[1]
                }
            }

class MatchesStream(StatbunkerStream):
    name = 'matches'
    path = 'competitions/LastMatches'
    schema_filepath = SCHEMAS_DIR / "matches.json"

    def get_url_params(self, context: Optional[dict]):
        params = dict()
        if 'comp_id' in self.config:
            params['comp_id'] = self.config['comp_id']
        else:
            raise Exception('comp_id is required')
        
        if 'club_id' in self.config:
            params['club_id'] = self.config['club_id']
        
        return params
    
    def validate_response(self, response: BeautifulSoup) -> None:
        lineups = response.find_all(class_ = 'matchLineup')
        
        if len(lineups) == 0:
            raise RetriableAPIError('Cannot find matches in page source')

        li = lineups[0].find_all('li')[0]
        spans = li.find_all('span')
        if len(spans) != 6:
            raise RetriableAPIError('Structure of results row is incorrect. It must have 6 spans')

        if len(spans[2].find_all('a')) != 1:
            raise RetriableAPIError('Unable to fetch id and comp_id because there are more than one links in vs span')
        
        if len(re.findall(r'\d+', spans[2].findChild()['href'])) != 6:
            raise RetriableAPIError('Unable to fetch id and comp_id because parse vs span link')
    
    def has_next_page(self) -> bool:
        try:
            self.driver.find_element(by=By.LINK_TEXT, value='›')
            return True
        except:
            return False
    
    def go_to_next_page(self) -> None:
        self.driver.find_element(by=By.LINK_TEXT, value='›').click()

    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        match_index = None
        if 'date' in self.config.keys():
            date = datetime.strptime(self.config['date'], '%d/%m/%Y')
            date_string = date.strftime('%A %-d %B %Y')

        match_lineups = response.find_all(class_ = 'matchLineup')
        for index, match_date in enumerate(response.find_all(class_ = 'slateGrey')):
            if 'date' in self.config:
                if date_string == match_date.get_text().strip():
                    match_index = index
            else:
                match_index = index
        
            if match_index is not None:
                match = match_lineups[match_index]
                for li in match.find_all('li'):
                    li_spans = li.find_all('span')
                    scores = [int(string.strip()) for string in li_spans[2].get_text().strip().split('-')]
                    numbers_in_link = re.findall(r'\d+', li_spans[2].findChild()['href'])
                    yield {
                        'id': int(numbers_in_link[3]),
                        'comp_id': int(numbers_in_link[2]),
                        'teams': {
                            'home': {
                                'id': int(re.findall(r'\d+', li_spans[1].find_all('a')[1]['href'])[1]),
                                'name': li_spans[1].get_text().strip()
                            },
                            'away': {
                                'id': int(re.findall(r'\d+', li_spans[3].find_all('a')[1]['href'])[1]),
                                'name': li_spans[3].get_text().strip()
                            }
                        },
                        'scores': {
                            'home': scores[0],
                            'away': scores[1]
                        },
                        'timestamp': datetime.strptime(f'{match_date.get_text().strip()} {li_spans[4].get_text().strip()}', '%A %d %B %Y %H:%M').isoformat()
                    }

class MatchDetailsStream(StatbunkerStream):
    name = 'match_details'
    primary_keys = ['id']
    schema_filepath = SCHEMAS_DIR / "match_details.json"

    @property
    def path(self):
        if 'comp_name' in self.config:
            return f'competitions/MatchDetails/{self.config["comp_name"].replace(" ", "-")}'
        else:
            raise Exception('comp_name is required')

    def get_url_params(self, context: Optional[dict]):
        params = dict()
        
        if 'comp_id' in self.config:
            params['comp_id'] = self.config['comp_id']
        else:
            raise Exception('comp_id is required')

        if 'match_id' in self.config:
            params['match_id'] = self.config['match_id']
        else:
            raise Exception('match_id is required')
        
        return params

    def validate_response(self, response: BeautifulSoup) -> None:
        if not response.find(class_ = 'matchReportTitle'):
            raise Exception('Cannot fetch team names because title bar is not present in page source')
        
        if len(response.find_all(class_ = 'matchReportInt')) != 5:
            raise Exception('Cannot extract goals information because stats containers is not complete in page source')

    def get_goal_object(self, sub_int):
        goal = dict()
        who_when_tag = sub_int.find('p')
        who_when_tag_words = who_when_tag.get_text().split(' ')
        how_words = sub_int.find('small').get_text().split(',')
        players = ' '.join(who_when_tag_words[1:]).split('=>')
        goal['minute'] = int(who_when_tag_words[0][:-1])
        if len(players) == 2:
            goal['scorer'] = players[1].strip()
            goal['assister'] = players[0].strip()
        else:
            goal['scorer'] = players[0].strip()
            goal['assister'] = None

        goal['foot'] = how_words[0].strip()        
        goal['shot_direction'] = how_words[1].strip().lower()
        goal['type'] = how_words[2].strip()
        goal['distance'] = int(how_words[3].split()[0])

        return goal


    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        title_bar = response.find(class_ = 'matchReportTitle')
        home_goals = list()
        away_goals = list()

        match_report_ints = response.find_all(class_ = 'matchReportInt')
        for sub_int in match_report_ints[1].find_all(class_ = 'matchReportSubInt'):
            home_goals.append(self.get_goal_object(sub_int))
        
        for sub_int in match_report_ints[2].find_all(class_ = 'matchReportSubInt'):
            away_goals.append(self.get_goal_object(sub_int))

        
        yield {
            'id': self.config['match_id'],
            'comp_id': self.config['comp_id'],
            'teams': {
                'home': title_bar.find(class_ = 'titleIntLeft').get_text().strip(),
                'away': title_bar.find(class_ = 'titleIntRight').get_text().strip()
            },
            'goals': {
                'home': home_goals,
                'away': away_goals
            }
        }



    




    



    


class GroupsStream(StatbunkerStream):
    """Define custom stream."""
    name = "groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()
