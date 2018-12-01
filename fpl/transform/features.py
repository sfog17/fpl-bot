import pandas as pd
from fpl.config import FILE_INTER_HISTORICAL, FILE_INTER_BOOTSTRAP_FEAT, FILE_INTER_FEATURES

LAST_SEASON = 'season_prev'

COL_HISTORICAL = ['element_code', 'season', 'total_points']
COL_HIST_RENAME = ['code', LAST_SEASON, 'points_mean_last_season']

MERGE_COLUMNS = ['code', LAST_SEASON]


def combine_boostrap_historic(df_bootstrap, df_historical):
    df_hist_trim = df_historical[COL_HISTORICAL]
    df_hist_trim.columns = COL_HIST_RENAME
    df_hist_trim['points_mean_last_season'] = df_hist_trim['points_mean_last_season'].apply(lambda x: round(x/38, 2))

    df_bootstrap[LAST_SEASON] = df_bootstrap['season'] - 1
    df_combined = df_bootstrap.merge(df_hist_trim, on=MERGE_COLUMNS, how='left')
    df_combined.drop(columns=[LAST_SEASON], inplace=True)

    return df_combined


def main():
    df_boostrap_feat = pd.read_csv(FILE_INTER_BOOTSTRAP_FEAT, encoding='utf-8-sig')
    df_historical = pd.read_csv(FILE_INTER_HISTORICAL)
    df_combined = combine_boostrap_historic(df_boostrap_feat, df_historical)
    df_combined.to_csv(FILE_INTER_FEATURES, index=False, encoding='utf-8-sig')
