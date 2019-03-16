import json
import logging
import numpy as np
import pandas as pd

import fpl.constants.fields as fld
from fpl.constants.structure import DIR_RAW_BOOTSTRAP, FILE_INTER_BOOTSTRAP

logger = logging.getLogger(__name__)


def get_player_info(bootstrap_json):
    """
    Take the json extracted from the api and returns a DataFrame with player info
    :param dict bootstrap_json:
    :return: pd.DataFrame
    """
    df_player = pd.DataFrame(bootstrap_json['elements'])
    mapping = {
        'id': fld.PLAYER_ID_SEASON,
        'web_name': fld.PLAYER_NAME,
        'team_code': fld.TEAM_ID,
        'status': fld.PLAYER_STATUS,
        'code': fld.PLAYER_ID,
        'now_cost': fld.PLAYER_COST,
        'chance_of_playing_next_round': fld.PLAYER_CHANCE_PLAY,
        'cost_change_event': fld.FPL_COST_CHANGE,
        'transfers_out_event': fld.FPL_TRANSFERS_OUT,
        'transfers_in_event': fld.FPL_TRANSFERS_IN,
        'event_points': fld.RESULT_POINTS_PREV,
        'minutes': fld.RESULT_MIN_CUMSUM_LAST,
        'element_type': fld.POSITION_ID,
        'team': fld.TEAM_ID_SEASON
    }
    if not set(mapping.keys()).issubset(set(df_player.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_player.columns)}')

    keep_fields = list(mapping.values())
    df_player.rename(index=str, columns=mapping, inplace=True)
    return df_player[keep_fields]


def get_position_info(bootstrap_json):
    """
    Take the json extracted from the api and returns a DataFrame with position info
    :param dict bootstrap_json:
    :return: pd.DataFrame
    """
    df_position = pd.DataFrame(bootstrap_json['element_types'])
    mapping = {
        'id': fld.POSITION_ID,
        'singular_name_short': fld.PLAYER_POSITION,
    }
    if not set(mapping.keys()).issubset(set(df_position.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_position.columns)}')

    keep_fields = list(mapping.values())
    df_position.rename(index=str, columns=mapping, inplace=True)
    return df_position[keep_fields]


def get_team_info(bootstrap_json):
    """
    Take the json extracted from the api and returns a DataFrame with team info
    :param dict bootstrap_json:
    :return: pd.DataFrame
    """

    df_team = pd.DataFrame(bootstrap_json['teams'])

    # Rename Fields
    mapping = {
        'id': fld.TEAM_ID_SEASON,
        'name': fld.TEAM_NAME,
        'code': fld.TEAM_ID,
        'strength': fld.TEAM_STRENGTH
    }
    if not set(mapping.keys()).issubset(set(df_team.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_team.columns)}')

    df_team.rename(index=str, columns=mapping, inplace=True)

    # Extract Data Games
    df_strengths = df_team[[fld.TEAM_ID_SEASON, fld.TEAM_NAME, fld.TEAM_STRENGTH]]

    df_team[fld.GAME_NB] = df_team['next_event_fixture'].apply(lambda x: len(x))
    df_team[fld.GAME_1_HOME] = df_team['next_event_fixture'].apply(lambda x: x[0]['is_home'] if len(x) > 0 else np.nan)
    df_team[fld.GAME_2_HOME] = df_team['next_event_fixture'].apply(lambda x: x[1]['is_home'] if len(x) > 1 else np.nan)

    # Join Game 1
    df_team['game_1_team_id_season'] = df_team['next_event_fixture'].apply(
        lambda x: x[0]['opponent'] if len(x) > 0 else np.nan)

    df_team = df_team.merge(df_strengths,
                            how='left', left_on='game_1_team_id_season', right_on=fld.TEAM_ID_SEASON,
                            suffixes=('', '_g1')
                            )
    # Join Game 2
    df_team['game_2_team_id_season'] = df_team['next_event_fixture'].apply(
        lambda x: x[1]['opponent'] if len(x) > 1 else np.nan)
    df_team = df_team.merge(df_strengths,
                            how='left', left_on='game_2_team_id_season', right_on=fld.TEAM_ID_SEASON,
                            suffixes=('', '_g2')
                            )
    # Rename new fields
    mapping_game = {
        fld.TEAM_NAME + '_g1': fld.GAME_1_TEAM_NAME,
        fld.TEAM_NAME + '_g1': fld.GAME_1_TEAM_STRENGTH,
        fld.TEAM_NAME + '_g2': fld.GAME_2_TEAM_NAME,
        fld.TEAM_STRENGTH + '_g2': fld.GAME_2_TEAM_STRENGTH
    }
    df_team.rename(index=str, columns=mapping_game, inplace=True)

    # Select Field
    keep_fields = list(mapping.values()) + list(mapping_game.values()) + [fld.GAME_NB, fld.GAME_1_HOME, fld.GAME_2_HOME]

    return df_team[keep_fields]


def get_week_info(file_path):
    """
    Read the file, extract info from json, denormalize into a dataframe
    :param pathlib.Path file_path:
    :return:
    """
    with open(file_path, encoding="utf8") as file_in:
        bootstrap_json = json.loads(file_in.read())

        # Merge Info Player / Team / Position
        df_player = get_player_info(bootstrap_json)
        df_team = get_team_info(bootstrap_json)
        df_position = get_position_info(bootstrap_json)

        logging.debug(df_player.transpose().head())
        logging.debug(df_team.transpose().head())
        logging.debug(df_position.transpose().head())

        # Add Gameweek + Season Info
        df_week = df_player.merge(df_team, on=fld.TEAM_ID).merge(df_position, on=fld.POSITION_ID)
        df_week[fld.GW] = bootstrap_json['next-event']
        df_week[fld.GW_PREV] = bootstrap_json['current-event']
        season_year = bootstrap_json['events'][0]['deadline_time'][:4]
        df_week[fld.SEASON_NAME] = season_year + '/' + str(int(season_year[2:4]) + 1)
        df_week[fld.SEASON_ID] = int(season_year) - 2006 + 1

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
            df_week = get_week_info(file_path)
            list_df.append(df_week)

    df_bootstrap = pd.concat(list_df)
    df_bootstrap.drop_duplicates(inplace=True)

    # Append the result  (requires to merge on the previous gamweek)
    df_result = df_bootstrap.copy()[[fld.SEASON_ID, fld.GW_PREV, fld.PLAYER_ID, fld.RESULT_POINTS_PREV]]
    df_result.rename(index=str,
                     columns={fld.RESULT_POINTS_PREV: fld.RESULT_POINTS, fld.GW_PREV: fld.GW},
                     inplace=True)

    df_final = df_bootstrap.merge(df_result,
                                  how='left',
                                  on=[fld.SEASON_ID, fld.GW, fld.PLAYER_ID]
                                  )
    return df_final


def run():
    df_bootstrap = build_bootstrap_dataset(DIR_RAW_BOOTSTRAP)
    df_bootstrap.to_csv(FILE_INTER_BOOTSTRAP, index=False, encoding='utf-8-sig')
