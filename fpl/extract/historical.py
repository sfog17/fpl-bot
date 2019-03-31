import json
import logging
import pandas as pd

import fpl.constants.fields as fld
from fpl.constants.structure import DIR_RAW_HIST, FILE_INTER_HISTORICAL

TOTAL_POINTS = 'total_points'

logger = logging.getLogger(__name__)


def add_avg_prev_season(df):
    """
    Calculate the average points for the previous season
    :param df: incoming dataframe (with player id, season id and total points)
    :return:
    """
    df_prev = df.copy()
    # Calculate average previous season
    df_prev[fld.TEAM_ID_SEASON] = df_prev[fld.TEAM_ID_SEASON] + 1
    df_prev[fld.STAT_POINTS_AVG_SEASON_PREV] = df_prev[TOTAL_POINTS].apply(lambda x: round(x / 38, 2))
    # Select only fields of interest
    df_prev = df_prev[[fld.PLAYER_ID, fld.TEAM_ID_SEASON, fld.STAT_POINTS_AVG_SEASON_PREV]]
    # Merge
    df_merged = df.merge(df_prev, how='left', on=[fld.PLAYER_ID, fld.TEAM_ID_SEASON])
    return df_merged


def get_historical_info(history_json):
    """
    Take the json extracted from the api and returns a DataFrame with historicql info
    :param dict history_json:
    :return: pd.DataFrame
    """
    df_history = pd.DataFrame(history_json['history_past'])
    mapping = {
        'element_code': fld.PLAYER_ID,
        'season': fld.TEAM_ID_SEASON
    }

    df_history.rename(columns=mapping, inplace=True)
    logging.debug(df_history.head())

    df_history = add_avg_prev_season(df_history)
    df_history = df_history[[fld.PLAYER_ID, fld.TEAM_ID_SEASON, fld.STAT_POINTS_AVG_SEASON_PREV]]

    return df_history



def build_bootstrap_dataset(dir_data_raw_hist):
    list_df = []
    for file_path in dir_data_raw_hist.glob('*.json'):
        logger.info(f'Processing file {file_path.name}')
        with open(file_path) as file_in:
            for idx, line in enumerate(file_in, start=1):
                logging.debug(f'File {file_path} - Line {idx}')
                player_json = json.loads(line)
                if player_json['history_past']:
                    df_history = get_historical_info(player_json)
                    list_df.append(df_history)
    df_all_history = pd.concat(list_df)
    df_all_history_clean = df_all_history.drop_duplicates()
    return df_all_history_clean


def run():
    output_historical = build_bootstrap_dataset(DIR_RAW_HIST)
    output_historical.to_csv(FILE_INTER_HISTORICAL, index=False)
