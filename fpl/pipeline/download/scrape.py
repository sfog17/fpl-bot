import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import requests
import pandas as pd
from fpl.constants.structure import DIR_RAW_PLAYER_DETAILS, DIR_MANAGER_HISTORY, DIR_RAW_BOOTSTRAP, DIR_RAW_FIXTURES

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


def scrape_manager_history(
        output_dir: Path = DIR_MANAGER_HISTORY,
        filename: str = 'manager',
        nb_managers: int = 2000,
        include_current: bool = False
) -> List[Path]:
    """
    Extract the manager for a list of random managers of fantasy football
    :param output_dir:   T  arget directory
    :param filename:        Filename output (date and suffix will be appended)
    :param nb_managers:        Number of managers to query
    :param include_current: If yes, extract the weekly rankings for the current season
    :return:                Path of the downloaded file
    """
    output_paths = []
    season_started = False

    logger.info('Start extracting manager')
    # Count number of players + get season_name
    bootstrap = requests.get(URL_BOOTSTRAP).json()
    season_name = _extract_season_name(bootstrap)
    season_started = bootstrap['events'][0]['finished']
    total_managers = bootstrap['total_players']
    managers_ids = [str(random.randint(0, total_managers)) for _ in range(nb_managers)]

    managers_current = []
    managers_past = []

    for idx, manager in enumerate(managers_ids, start=1):
        if (idx % 100) == 0:
            logger.debug(f'Extracted {idx} Managers')
        result = requests.get(URL_BASE+'entry/' + manager + '/history/')

        # Extract info current
        if include_current:
            current = result.json()['current']
            for gw in range(len(current)):
                current[gw]['manager'] = manager
                current[gw]['season_name'] = season_name
            managers_current.extend(current)

        # Extract info past
        past = result.json()['past']
        for gw in range(len(past)):
            past[gw]['manager'] = manager
        managers_past.extend(past)

        time.sleep(0.01)
    logger.info('Finished extracting manager')

    # Output data current (if selected)
    if include_current and season_started:
        output_path_current = output_dir / _format_filename(filename + '_current', 'csv')
        df_rankings_current = pd.DataFrame(managers_current).sort_values(['manager', 'event'])
        df_rankings_current.to_csv(output_path_current, index=False)
        logger.info(f'Results (current) wrote to csv file {output_path_current}')
        output_paths.append(output_path_current)

    # Output data past
    output_path_past = output_dir / _format_filename(filename + '_past', 'csv')
    df_rankings_past = pd.DataFrame(managers_past).sort_values(['manager', 'season_name'])
    df_rankings_past.to_csv(output_path_past, index=False)
    logger.info(f'Results (past) wrote to csv file {output_path_past}')
    output_paths.append(output_path_past)

    return output_paths


def scrape_manager_team(email: str, password: str) -> dict:
    logging.info('Connect - Retrieve manager info from https://fantasy.premierleague.com/')

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
    manager_info = response.json()
    logging.debug(f'Manager Info: \n {manager_info}')
    return manager_info


def _format_filename(output_name, extension) -> str:
    today_str = datetime.today().strftime('%Y%m%d')
    full_name = today_str + '_' + output_name + '.' + extension
    return full_name


def _extract_season_name(bootstrap_json: Dict[str, Any]) -> str:
    events = bootstrap_json['events']
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
        NB_MANAGERS = int(sys.argv[2])
        scrape_manager_history(nb_managers=NB_MANAGERS)
    elif DATA == 'history-include-current':
        NB_MANAGERS = int(sys.argv[2])
        scrape_manager_history(nb_managers=NB_MANAGERS, include_current=True)
    else:
        print('Wrong argument')
