import logging
import random
import time
from datetime import datetime
import requests
import pandas as pd
from fpl.constants import DIR_RAW_HIST, DIR_RANKINGS, URL_BASE, URL_BOOTSTRAP, URL_PLAYERS

# Fields BOOTSTRAP
ELEMENTS = 'elements'
ID = 'id'

logger = logging.getLogger(__name__)


def format_filename(output_name, extension):
    today_str = datetime.today().strftime('%y%m%d')
    full_name = today_str + '_' + output_name + '.' + extension
    return full_name


def get_historical(output_name):
    """
    Extract the historical data for all the players

    Arguments:
        output_name {str} -- Name of the output file (only filename, no path nor extension)
    """

    with open(DIR_RAW_HIST / format_filename(output_name, 'json'), 'w') as f_out:

        logger.info('Start extracting historical data')

        resp_bootstrap = requests.get(URL_BOOTSTRAP)

        for player in resp_bootstrap.json()[ELEMENTS]:
            player_id = player[ID]
            resp = requests.get(f'{URL_PLAYERS}/{player_id}')
            if resp.status_code == 200:
                logger.debug(f'Player {player_id} - Success')
                f_out.writelines(resp.text + '\n')
            else:
                logger.warning(f'Player {player_id} - Error : {resp.status_code}')

            time.sleep(0.1)

        logger.info('Finished extracting historical data')


def get_rankings(output_name, nb_users):
    """
    Extract the rankings for a list of random users of fantasy football

    Arguments:
        output_name {str} -- Name of the output file (only filename, no path nor extension)
        nb_users {int} -- Number of users to query
    """

    logger.info('Start extracting rankings')

    total_users = requests.get(URL_BOOTSTRAP).json()['total-players']
    list_users_ranks = []

    for i in range(nb_users):
        if (i % 100) == 0:
            logger.debug(f'Extracted {i} Users')
        
        user_id = str(random.randint(0, total_users))
        result = requests.get(URL_BASE+'/entry/' + user_id + '/history')
        list_users_ranks.extend(result.json()['history'])
        time.sleep(0.01)

    logger.info('Finished extracting rankings')

    df_rankings = pd.DataFrame(list_users_ranks).sort_values('event')
    output_path = DIR_RANKINGS / format_filename(output_name, 'csv')
    df_rankings.to_csv(output_path, index=False)

    logger.info(f'Results wrote to csv file {output_path}')
