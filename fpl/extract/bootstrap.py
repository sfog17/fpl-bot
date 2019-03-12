import json
import logging
import numpy as np
import pandas as pd

import fpl.constants.fields as field
from fpl.constants.structure import DIR_RAW_BOOTSTRAP, FILE_INTER_BOOTSTRAP

logger = logging.getLogger(__name__)

F_SEASON_ID = 'season'
F_SEASON_NAME = 'season_name'
F_GAMEWEEK = 'gameweek'
F_GAMEWEEK_PREV = 'gameweek_prev'


def get_player_info(bootstrap_json):
    """
    Take the json extracted from the api and returns a DataFrame with player info
    :param dict bootstrap_json:
    :return: pd.DataFrame
    """
    df_api = pd.DataFrame(bootstrap_json['elements'])
    mapping = {
        'id': field.PLAYER_ID_SEASON,
        'web_name': field.PLAYER_NAME,
        'team_code': field.TEAM_ID,
        'status': field.PLAYER_STATUS,
        'code': field.PLAYER_ID,
        'now_cost': field.PLAYER_COST,
        'chance_of_playing_next_round': field.PLAYER_CHANCE_PLAY,
        'cost_change_event': field.FPL_COST_CHANGE,
        'transfers_out_event': field.FPL_TRANSFERS_OUT,
        'transfers_in_event': field.FPL_TRANSFERS_IN,
        'event_points': field.RESULT_POINTS_LAST,
        'minutes': field.RESULT_MIN_CUMSUM_LAST,
        'element_type': field.POSITION_ID,
        'team': field.TEAM_ID_SEASON
    }
    if not set(mapping.keys()).issubset(set(df_api.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_api.columns)}')

    selected_fields = list(mapping.values())
    df_api.rename(index=str, columns=mapping, inplace=True)
    return df_api[selected_fields]


def get_team_info(df):
    df_teams = df.copy()
    df_teams['game_nb'] = df_teams['next_event_fixture'].apply(lambda x: len(x))
    df_teams['game_1_home'] = df_teams['next_event_fixture'].apply(lambda x: x[0]['is_home'] if len(x)>0 else np.nan)
    df_teams['game_1_team_id_season'] = df_teams['next_event_fixture'].apply(lambda x: x[0]['opponent'] if len(x)>0 else np.nan)
    df_teams['game_2_home'] = df_teams['next_event_fixture'].apply(lambda x: x[1]['is_home'] if len(x)>1 else np.nan)
    df_teams['game_2_team_id_season'] = df_teams['next_event_fixture'].apply(lambda x: x[1]['opponent'] if len(x)>1 else np.nan)

    df_strengths = df_teams[['id', 'name', 'strength']]
    df_teams = df_teams.merge(df_strengths, 
                              how='left', left_on='game_1_team_id_season', right_on='id',
                              suffixes=('', '_g1')
                              ).merge(
                                df_strengths, 
                                how='left', left_on='game_2_team_id_season', right_on='id',
                                suffixes=('', '_g2')
    )
    
    df_teams = df_teams[['code', 'name', 'strength', 'game_nb',
                         'name_g1', 'game_1_home', 'strength_g1',
                         'name_g2', 'game_2_home', 'strength_g2']]
    
    df_teams.columns = ['team_code', 'team_name', 'team_strength', 'game_nb',
                        'game_1_team_name', 'game_1_home', 'game_1_team_strength',
                        'game_2_team_name', 'game_2_home', 'game_2_team_strength']
    
    return df_teams


def combine(df_players, df_teams, df_positions):
    df_teams = get_team_info(df_teams)
    df_positions = df_positions[['id', 'singular_name_short']]
    df_positions.columns = ['element_type', 'player_position']
    df_week = df_players.merge(df_teams, on='team_code').merge(df_positions, on='element_type')
    return df_week


def get_week_info(file_path):
    # df_players = pd.DataFrame(bootstrap_json['elements'])
    # df_teams = pd.DataFrame(bootstrap_json['teams'])
    # df_positions = pd.DataFrame(bootstrap_json['element_types'])
    # df_week = combine(df_players, df_teams, df_positions)
    #
    # df_week[F_GAMEWEEK] = bootstrap_json['next-event']
    # df_week[F_GAMEWEEK_PREV] = bootstrap_json['current-event']
    #
    # season_year = bootstrap_json['events'][0]['deadline_time'][:4]
    # df_week[F_SEASON_NAME] = season_year + '/' + str(int(season_year[2:4]) + 1)
    # df_week[F_SEASON_ID] = int(season_year) - 2006 + 1
    with open(file_path, encoding="utf8") as file_in:
        bootstrap_json = json.loads(file_in.read())
        df_week = get_player_info(bootstrap_json)

    return df_week


def build_bootstrap_dataset(dir_data_raw_hist):
    """
    :param pathlib.Path dir_data_raw_hist:
    :return:
    """
    list_df = []
    logger.info(f'Screen folder {dir_data_raw_hist}')
    for file_path in dir_data_raw_hist.glob('*.json'):
        if file_path.is_file():
            logger.info(f'Processing file {file_path.name}')
            df_bootstrap = get_week_info(file_path)
            list_df.append(df_bootstrap)

    df_all_bootstrap = pd.concat(list_df)
    df_all_bootstrap_clean = df_all_bootstrap.drop_duplicates()
    return df_all_bootstrap_clean


def run():
    df_bootstrap = build_bootstrap_dataset(DIR_RAW_BOOTSTRAP)
    df_bootstrap.to_csv(FILE_INTER_BOOTSTRAP, index=False, encoding='utf-8-sig')


