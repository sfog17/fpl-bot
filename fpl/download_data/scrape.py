import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import requests
import pandas as pd
from fpl.constants.structure import DIR_RAW_PLAYER_DETAILS, DIR_USER_HISTORY, DIR_RAW_BOOTSTRAP, DIR_RAW_FIXTURES

# URL
URL_BASE = 'https://fantasy.premierleague.com/api/'
URL_BOOTSTRAP = URL_BASE + 'bootstrap-static/'
URL_PLAYERS = URL_BASE + 'element-summary'
URL_FIXTURES = URL_BASE + 'fixtures/'

logger = logging.getLogger(__name__)


def scrape_bootstrap(output_dir: Path = DIR_RAW_BOOTSTRAP, filename: str = 'bootstrap_static') -> Path:
    """
    Fetch bootstrap data (all info from the current gameweek)
    :param output_dir:   Target directory
    :param filename:     Filename output (date and suffix will be appended)
    :return:             Path of the downloaded file
    """
    output_dir.mkdir(parents=True, exist_ok=True)     # Create directory if doesn't exist
    output_path = output_dir / _format_filename(filename, 'json')

    response = requests.get(URL_BOOTSTRAP)
    if response.status_code == 200 and response.text != '{}':
        with output_path.open('w', encoding='utf-8') as f_out:
            f_out.write(response.text)
        logger.info(f'Bootstrap data written into {output_path.resolve().absolute()}')
        return output_path
    else:
        raise requests.exceptions.InvalidURL(URL_BOOTSTRAP)


def scrape_fixtures(output_dir: Path = DIR_RAW_FIXTURES, filename: str = 'fixtures') -> Path:
    """
    Fetch fixtures data (all games of the season)
    :param output_dir:   Target directory
    :param filename:     Filename output (date and suffix will be appended)
    :return:             Path of the downloaded file
    """
    output_dir.mkdir(parents=True, exist_ok=True)     # Create directory if doesn't exist
    output_path = output_dir / _format_filename(filename, 'json')

    response = requests.get(URL_FIXTURES)
    if response.status_code == 200 and response.text != '{}':
        with output_path.open('w', encoding='utf-8') as f_out:
            f_out.write(response.text)
        return output_path
    else:
        raise requests.exceptions.InvalidURL(URL_FIXTURES)


def scrape_player_detailed(output_dir: Path = DIR_RAW_PLAYER_DETAILS, filename: str = 'player_detailed_data') -> Path:
    """
    Extract the detailed data (including player-details) for all the players
    :param output_dir:   Target directory
    :param filename:     Filename output (date and suffix will be appended)
    :return:             Path of the downloaded file
    """
    output_dir.mkdir(parents=True, exist_ok=True)     # Create directory if doesn't exist
    output_path = output_dir / _format_filename(filename, 'json')

    with open(output_path, 'w') as f_out:
        logger.info('Start extracting player-details data')
        resp_bootstrap = requests.get(URL_BOOTSTRAP)
        for player in resp_bootstrap.json()['elements']:
            player_id = player['id']
            resp = requests.get(f'{URL_PLAYERS}/{player_id}/')
            if resp.status_code == 200 and resp.text != '{}':
                logger.debug(f'Player {player_id} - Success')
                f_out.writelines(resp.text + '\n')
            else:
                logger.warning(f'Player {player_id} - Error : {resp.status_code}')
                raise requests.exceptions.InvalidURL(f'{URL_PLAYERS}/{player_id}')

            time.sleep(0.1)

        logger.info('Finished extracting player-details data')
    return output_path


def scrape_user_history(
        output_dir: Path = DIR_USER_HISTORY,
        filename: str = 'user_history',
        nb_users: int = 2000,
        include_current: bool = False
) -> List[Path]:
    """
    Extract the user_history for a list of random users of fantasy football
    :param output_dir:   T  arget directory
    :param filename:        Filename output (date and suffix will be appended)
    :param nb_users:        Number of users to query
    :param include_current: If yes, extract the weekly rankings for the current season
    :return:                Path of the downloaded file
    """
    output_paths = []

    logger.info('Start extracting user_history')
    # Count number of players + get season_name
    bootstrap = requests.get(URL_BOOTSTRAP).json()
    season_name = _extract_season_name(bootstrap)
    total_users = bootstrap['total_players']
    users_id = [str(random.randint(0, total_users)) for _ in range(nb_users)]

    users_current = []
    users_past = []

    for idx, user in enumerate(users_id, start=1):
        if (idx % 100) == 0:
            logger.debug(f'Extracted {idx} Users')
        result = requests.get(URL_BASE+'entry/' + user + '/history/')

        # Extract info current
        if include_current:
            current = result.json()['current']
            for gw in range(len(current)):
                current[gw]['user'] = user
                current[gw]['season_name'] = season_name
            users_current.extend(current)

        # Extract info past
        past = result.json()['past']
        for gw in range(len(past)):
            past[gw]['user'] = user
        users_past.extend(past)

        time.sleep(0.01)
    logger.info('Finished extracting user_history')

    # Output data current (if selected)
    if include_current:
        output_path_current = output_dir / _format_filename(filename + '_current', 'csv')
        df_rankings_current = pd.DataFrame(users_current).sort_values(['user', 'event'])
        df_rankings_current.to_csv(output_path_current, index=False)
        logger.info(f'Results (current) wrote to csv file {output_path_current}')
        output_paths.append(output_path_current)

    # Output data past
    output_path_past = output_dir / _format_filename(filename + '_past', 'csv')
    df_rankings_past = pd.DataFrame(users_past).sort_values(['user', 'season_name'])
    df_rankings_past.to_csv(output_path_past, index=False)
    logger.info(f'Results (past) wrote to csv file {output_path_past}')
    output_paths.append(output_path_past)

    return output_paths


def scrape_user_team(email: str, password: str) -> dict:
    logging.info('Connect - Retrieve user info from https://fantasy.premierleague.com/')

    # Create Session
    session = requests.Session()
    # Go to login page to get the cookies
    session.get('https://fantasy.premierleague.com/')
    csrftoken = session.cookies['csrftoken']
    login_data = {
        'csrfmiddlewaretoken': csrftoken,
        'login': email,
        'password': password,
        'app': 'plfpl-web',
        'redirect_uri': 'https://fantasy.premierleague.com/a/login'
    }
    # Log in
    session.post('https://users.premierleague.com/accounts/login/', data=login_data)
    # Get List Transfers
    response = session.get('https://fantasy.premierleague.com/drf/transfers')
    user_info = response.json()
    logging.debug(f'User Info: \n {user_info}')
    return user_info


def _format_filename(output_name, extension):
    today_str = datetime.today().strftime('%Y%m%d')
    full_name = today_str + '_' + output_name + '.' + extension
    return full_name


def _extract_season_name(boostrap_json: Dict[str, Any]):
    events = boostrap_json['events']
    year_start = events[0]['deadline_time'][:4]
    year_end = events[-1]['deadline_time'][:4]
    season_name = f'{year_start}/{year_end[2:]}'
    return season_name


if __name__ == '__main__':
    DATA = sys.argv[1]
    if DATA == 'bootstrap':
        scrape_bootstrap()
    elif DATA == 'players':
        scrape_player_detailed()
    elif DATA == 'fixtures':
        scrape_fixtures()
    elif DATA == 'history':
        NB_USERS = int(sys.argv[2])
        scrape_user_history(nb_users=NB_USERS)
    elif DATA == 'history-include-current':
        NB_USERS = int(sys.argv[2])
        scrape_user_history(nb_users=NB_USERS, include_current=True)
    else:
        print('Wrong argument')
