import json
import logging
import numpy as np
import pandas as pd
from fpl.constants import DIR_RAW_BOOTSTRAP, FILE_INTER_BOOTSTRAP

logger = logging.getLogger(__name__)


F_SEASON_ID = 'season'
F_SEASON_NAME = 'season_name'
F_GAMEWEEK = 'gameweek'
F_GAMEWEEK_PREV = 'gameweek_prev'


def process_data_team(df):
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
    df_teams = process_data_team(df_teams)
    df_positions = df_positions[['id', 'singular_name_short']]
    df_positions.columns = ['element_type', 'player_position']
    df_week = df_players.merge(df_teams, on='team_code').merge(df_positions, on='element_type')
    return df_week


def json_to_df(bootstrap_json):
    df_players = pd.DataFrame(bootstrap_json['elements'])
    df_teams = pd.DataFrame(bootstrap_json['teams'])
    df_positions = pd.DataFrame(bootstrap_json['element_types'])
    df_week = combine(df_players, df_teams, df_positions)

    df_week[F_GAMEWEEK] = bootstrap_json['next-event']
    df_week[F_GAMEWEEK_PREV] = bootstrap_json['current-event']

    season_year = bootstrap_json['events'][0]['deadline_time'][:4]
    df_week[F_SEASON_NAME] = season_year + '/' + str(int(season_year[2:4]) + 1)
    df_week[F_SEASON_ID] = int(season_year) - 2006 + 1

    return df_week


def filepath_to_df(file_path):
    with open(file_path, encoding="utf8") as file_in:
        bootstrap_json = json.loads(file_in.read())
        df_week = json_to_df(bootstrap_json)
    return df_week


def all_bootstrap_to_df(dir_data_raw_hist):
    """
    :param pathlib.Path dir_data_raw_hist:
    :return:
    """
    list_df = []
    for file_path in dir_data_raw_hist.glob('*.json'):
        if file_path.is_file():
            logger.info(f'Processing file {file_path.name}')
            df_bootstrap = filepath_to_df(file_path)
            list_df.append(df_bootstrap)

    df_all_bootstrap = pd.concat(list_df)
    df_all_bootstrap_clean = df_all_bootstrap.drop_duplicates()
    return df_all_bootstrap_clean


def run():
    df_bootstrap = all_bootstrap_to_df(DIR_RAW_BOOTSTRAP)
    df_bootstrap.to_csv(FILE_INTER_BOOTSTRAP, index=False, encoding='utf-8-sig')


