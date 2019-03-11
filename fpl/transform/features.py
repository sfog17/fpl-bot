import pandas as pd
from fpl.constants.structure import FILE_INTER_HISTORICAL, FILE_INTER_BOOTSTRAP_FEAT, FILE_INTER_FEATURES
from fpl.utils.featurise import add_prev_groupby

F_SEASON_ID = 'season_id'
F_HIST_POINTS_AVG = 'points_mean_last_season'
F_PLAYER_ID = 'player_id'


def combine_boostrap_historic(df_bootstrap, df_historical):
    last_season = 'season_prev'
    df_hist_trim = df_historical['element_code', 'season', 'total_points']
    df_hist_trim.columns = [F_PLAYER_ID, last_season, F_HIST_POINTS_AVG]
    df_hist_trim[F_HIST_POINTS_AVG] = df_hist_trim[F_HIST_POINTS_AVG].apply(lambda x: round(x/38, 2))

    df_bootstrap[last_season] = df_bootstrap[F_SEASON_ID] - 1
    df_combined = df_bootstrap.merge(df_hist_trim, on=[F_PLAYER_ID, last_season], how='left')
    df_combined.drop(columns=[last_season], inplace=True)

    return df_combined


def preprocess(df_bootstrap):
    df = df_bootstrap.copy()
    # Rename fields
    df
    # Select Fields

    # Merge Historical

    # Get Features - Other module


def main():
    df_boostrap_feat = pd.read_csv(FILE_INTER_BOOTSTRAP_FEAT, encoding='utf-8-sig')
    df_historical = pd.read_csv(FILE_INTER_HISTORICAL)
    df_combined = combine_boostrap_historic(df_boostrap_feat, df_historical)
    df_combined.to_csv(FILE_INTER_FEATURES, index=False, encoding='utf-8-sig')


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


def run2():
    df_bootstrap_trim = trim_bootstrap(df_bootstrap)
    df_bootstrap_trim.to_csv(FILE_INTER_BOOTSTRAP_FEAT, index=False, encoding='utf-8-sig')
