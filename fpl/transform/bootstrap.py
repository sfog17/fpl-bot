import json
import logging
import numpy as np
import pandas as pd
from fpl.config import DIR_RAW_BOOTSTRAP, FILE_INTER_BOOTSTRAP, FILE_INTER_BOOTSTRAP_FEAT

logger = logging.getLogger(__name__)

def process_data_team(df):
    df_teams = df.copy()
    df_teams['nb_games'] = df_teams['next_event_fixture'].apply(lambda x: len(x))
    df_teams['home_g1'] = df_teams['next_event_fixture'].apply(lambda x: x[0]['is_home'] if len(x)>0 else np.nan)
    df_teams['opp_g1'] = df_teams['next_event_fixture'].apply(lambda x: x[0]['opponent'] if len(x)>0 else np.nan)
    df_teams['home_g2'] = df_teams['next_event_fixture'].apply(lambda x: x[1]['is_home'] if len(x)>1 else np.nan)
    df_teams['opp_g2'] = df_teams['next_event_fixture'].apply(lambda x: x[1]['opponent'] if len(x)>1 else np.nan)

    df_strengths = df_teams[['id', 'name', 'strength']]
    df_teams = df_teams.merge(df_strengths, 
                              how='left', left_on='opp_g1', right_on='id',
                              suffixes=('', '_g1')
                              ).merge(
                                df_strengths, 
                                how='left', left_on='opp_g2', right_on='id',
                                suffixes=('', '_g2')
    )
    
    df_teams = df_teams[['code', 'name', 'strength', 'nb_games',
                         'name_g1', 'home_g1', 'strength_g1',
                         'name_g2', 'home_g2', 'strength_g2']]
    
    df_teams.columns = ['team_code', 'team_name', 'team_strength', 'nb_games',
                        'name_g1', 'home_g1', 'strength_g1',
                        'name_g2', 'home_g2', 'strength_g2']
    
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

    df_week['gameweek'] = bootstrap_json['next-event']
    df_week['prev_gameweek'] = bootstrap_json['current-event']

    season_year = bootstrap_json['events'][0]['deadline_time'][:4]
    df_week['season_name'] = season_year + '/' + str(int(season_year[2:4]) + 1)
    df_week['season'] = int(season_year) - 2006 + 1

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


def resample_shift_prev(df, col_index, col_shift):
    df_index = df.set_index(col_index)
    new_index = range(min(df_index.index), max(df_index.index) + 1)
    df_resampled = df_index.reindex(new_index)
    df_shift = df_resampled.shift(1)[col_shift]
    return df_shift


def add_prev_groupby(df_init, col_group, col_sort, col_shift):
    indexes = col_group + [col_sort]
    s_next = df_init.groupby(col_group).apply(lambda x: resample_shift_prev(x, col_index=col_sort, col_shift=col_shift))
    df_next = s_next.reset_index(level=indexes)

    # Set columns to merge as objects, otherwise pandas  might change column dtype then complain it can't merge
    df_init[indexes] = df_init[indexes].apply(lambda x: x.astype('object'))
    df_next[indexes] = df_next[indexes].apply(lambda x: x.astype('object'))

    df_final = df_init.merge(df_next, how='left', on=indexes, suffixes=('', '_prev'))
    return df_final


def trim_bootstrap(df_bootstrap):
    """
    Select columns of interest and initialise values to 0 for week 1
    :param pd.DataFrame df_bootstrap:
    :return:
    """

    # Define Type
    df_bootstrap = df_bootstrap.apply(lambda x: x.astype('object'))
    # p_float = ['influence', 'creativity', 'threat', 'selected_by_percent']
    # df_bootstrap[p_float] = df_bootstrap[p_float].apply(lambda x: x.astype('float'))

    # Select Columns -  Info is for the week (chance of playing, transfers)
    #                   Result is for the end of the gameweek
    col_info_week = ['season_name', 'season', 'gameweek',
                     'code', 'id', 'web_name', 'team_name', 'player_position', 'now_cost',
                     # 'event_points',
                     'chance_of_playing_next_round', 'status',
                     'nb_games', 'name_g1', 'home_g1', 'strength_g1', 'name_g2', 'home_g2', 'strength_g2',
                     'transfers_in_event', 'transfers_out_event', 'selected_by_percent']
    df_info_week = df_bootstrap[col_info_week]
    df_info_week = df_info_week.dropna(subset=['gameweek'])

    col_result = ['season_name', 'prev_gameweek', 'code',
                  'event_points',
                  # 'minutes', 'creativity', 'influence', 'threat'
                  ]
    df_result = df_bootstrap[col_result]

    # Fill in chance of playing - when value is null
    df_info_week['chance_of_playing_next_round'] = df_info_week['chance_of_playing_next_round'].fillna(100)

    # Combine info and result
    df_boot_clean = df_info_week.merge(df_result,
                                       how='left',
                                       left_on=['season_name', 'gameweek', 'code'],
                                       right_on=['season_name', 'prev_gameweek', 'code'],
                                       suffixes=('_prev', '')
                                       )
    df_boot_clean = df_boot_clean.drop(columns=['prev_gameweek'])

    # Rename columns
    col_rename = {
        'event_points': 'points',
        'now_cost': 'cost',
        'chance_of_playing_next_round': 'chance_of_playing'
    }
    df_boot_clean = df_boot_clean.rename(columns=col_rename)

    # Add Previous
    df_boot_clean = add_prev_groupby(df_boot_clean, col_group=['season_name', 'code'],
                                     col_sort='gameweek', col_shift='points')

    return df_boot_clean


def main():
    df_bootstrap = all_bootstrap_to_df(DIR_RAW_BOOTSTRAP)
    df_bootstrap.to_csv(FILE_INTER_BOOTSTRAP, index=False, encoding='utf-8-sig')
    df_bootstrap_trim = trim_bootstrap(df_bootstrap)
    df_bootstrap_trim.to_csv(FILE_INTER_BOOTSTRAP_FEAT, index=False, encoding='utf-8-sig')
