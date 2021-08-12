import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Literal
import numpy as np
import pandas as pd
import fpl.constants.fields as fld
import fpl.constants.structure as struc

logger = logging.getLogger(__name__)


TEAM_FIELDS = [
    fld.TEAM_NAME, fld.TEAM_ID, fld.TEAM_STRENGTH, fld.GAME_NB,
    fld.GAME_1_HOME, fld.GAME_1_TEAM_NAME, fld.GAME_1_TEAM_STRENGTH,
    fld.GAME_2_HOME, fld.GAME_2_TEAM_NAME, fld.GAME_2_TEAM_STRENGTH
]


def get_player_info(bootstrap_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Take the json extracted from the api and returns a DataFrame with player info
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
        'selected_by_percent': fld.FPL_SELECTED_BY,
        'transfers_out_event': fld.FPL_TRANSFERS_OUT,
        'transfers_in_event': fld.FPL_TRANSFERS_IN,
        'ep_next': fld.FPL_EXPECTED_POINTS,
        'influence': fld.FPL_INFLUENCE,
        'creativity': fld.FPL_CREATIVITY,
        'threat': fld.FPL_THREAT,
        'ict_index': fld.FPL_ICT_INDEX,
        'event_points': fld.RESULT_POINTS_PREV,
        'minutes': fld.STAT_MINUTES_TOTAL_SEASON,
        'element_type': fld.POSITION_ID,
        'team': fld.TEAM_ID_SEASON
    }
    if not set(mapping.keys()).issubset(set(df_player.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_player.columns)}')

    keep_fields = list(mapping.values())
    df_player.rename(columns=mapping, inplace=True)
    return df_player[keep_fields]


def get_position_info(bootstrap_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Take the json extracted from the api and returns a DataFrame with position info
    """
    df_position = pd.DataFrame(bootstrap_json['element_types'])
    mapping = {
        'id': fld.POSITION_ID,
        'singular_name_short': fld.PLAYER_POSITION,
    }
    if not set(mapping.keys()).issubset(set(df_position.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_position.columns)}')

    keep_fields = list(mapping.values())
    df_position.rename(columns=mapping, inplace=True)
    return df_position[keep_fields]


def get_team_info_v1(bootstrap_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Take the json extracted from the api and returns a DataFrame with team info
    Valid for API until 2020
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

    df_team.rename(columns=mapping, inplace=True)

    # Extract Data Games
    df_strengths = df_team[[fld.TEAM_ID_SEASON, fld.TEAM_NAME, fld.TEAM_STRENGTH]]

    df_team[fld.GAME_NB] = df_team['next_event_fixture'].apply(lambda x: len(x))
    df_team[fld.GAME_1_HOME] = df_team['next_event_fixture'].apply(lambda x: x[0]['is_home'] if len(x) > 0 else np.nan)
    df_team[fld.GAME_2_HOME] = df_team['next_event_fixture'].apply(lambda x: x[1]['is_home'] if len(x) > 1 else np.nan)

    # Join Game 1
    df_team['game_1_team_id_season'] = df_team['next_event_fixture'].apply(
        lambda x: x[0]['opponent'] if len(x) > 0 else np.nan)

    df_team = df_team.merge(df_strengths.rename(columns={fld.TEAM_ID_SEASON: 'game_1_team_id_season'}),
                            how='left', on='game_1_team_id_season',
                            suffixes=('', '_g1')
                            )
    # Join Game 2
    df_team['game_2_team_id_season'] = df_team['next_event_fixture'].apply(
        lambda x: x[1]['opponent'] if len(x) > 1 else np.nan)

    df_team = df_team.merge(df_strengths.rename(columns={fld.TEAM_ID_SEASON: 'game_2_team_id_season'}),
                            how='left', on='game_2_team_id_season',
                            suffixes=('', '_g2')
                            )
    # Rename new fields
    mapping_game = {
        (fld.TEAM_NAME + '_g1'): fld.GAME_1_TEAM_NAME,
        (fld.TEAM_STRENGTH + '_g1'): fld.GAME_1_TEAM_STRENGTH,
        (fld.TEAM_NAME + '_g2'): fld.GAME_2_TEAM_NAME,
        (fld.TEAM_STRENGTH + '_g2'): fld.GAME_2_TEAM_STRENGTH
    }
    df_team.rename(columns=mapping_game, inplace=True)

    return df_team[TEAM_FIELDS]


def get_team_info_v2(bootstrap_json: Dict[str, Any], fixture_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Take the json extracted from the api and returns a DataFrame with team info
    Valid for API after 2020
    """
    # Get Team Data
    df_team = pd.DataFrame(bootstrap_json['teams'])
    mapping = {
        'id': fld.TEAM_ID_SEASON,
        'name': fld.TEAM_NAME,
        'code': fld.TEAM_ID,
        'strength': fld.TEAM_STRENGTH
    }
    if not set(mapping.keys()).issubset(set(df_team.columns)):
        raise KeyError(f'{set(mapping.keys()) - set(df_team.columns)}')
    df_team.rename(columns=mapping, inplace=True)
    keep_fields = [fld.TEAM_NAME, fld.TEAM_ID, fld.TEAM_ID_SEASON, fld.TEAM_STRENGTH]
    df_team = df_team[keep_fields]
    # Get next gameweek fixtures
    season_name = extract_season_name(bootstrap_json)
    prev_gw = extract_prev_gameweek(bootstrap_json)
    current_gw = get_next_game(prev_gw, season_name)
    if current_gw is None:
        current_gw = prev_gw
    df_fixtures = get_fixtures_info(fixture_json, current_gw)
    # Format with next gameweek data
    df_format_gw1 = df_team\
        .set_index(fld.TEAM_ID_SEASON)\
        .join(df_fixtures.set_index(fld.TEAM_ID_SEASON)) \
        .reset_index()\
        .merge(
            df_team.rename(
                columns={
                    fld.TEAM_NAME: fld.GAME_1_TEAM_NAME,
                    fld.TEAM_STRENGTH: fld.GAME_1_TEAM_STRENGTH
                }
            ),
            left_on=fld.GAME_1_TEAM_ID_SEASON,
            right_on=fld.TEAM_ID_SEASON,
            how='left',
            suffixes=['', '_1']
        )
    df_format_gw2 = df_format_gw1.merge(
            df_team.rename(
                columns={
                    fld.TEAM_NAME: fld.GAME_2_TEAM_NAME,
                    fld.TEAM_STRENGTH: fld.GAME_2_TEAM_STRENGTH
                }
            ),
            left_on=fld.GAME_2_TEAM_ID_SEASON,
            right_on=fld.TEAM_ID_SEASON,
            how='left',
            suffixes=['', '_2']
        )

    return df_format_gw2[TEAM_FIELDS]


def get_fixtures_info(fixture_json: Dict[str, Any], gameweek: int) -> pd.DataFrame:
    """
    Extra the fixtures information
    Required to identify the next game
    """
    fixtures_data = []
    df = pd.DataFrame(fixture_json)
    df_gw = df[df['event'] == gameweek]
    teams_playing = set(df_gw['team_h']).union(set(df_gw['team_a']))
    for team in teams_playing:
        team_data = []
        for home_against in df_gw[(df_gw['team_h'] == team)]['team_a'].to_list():
            team_data.append((True, home_against))
        for away_against in df_gw[(df_gw['team_a'] == team)]['team_h'].to_list():
            team_data.append((False, away_against))
        game_nb = len(team_data)
        aggregated = {
            fld.TEAM_ID_SEASON: team,
            fld.GAME_NB: game_nb,
            fld.GAME_1_HOME: team_data[0][0],
            fld.GAME_1_TEAM_ID_SEASON: team_data[0][1]
        }
        if game_nb == 2:
            aggregated.update({
                fld.GAME_2_HOME: team_data[1][0],
                fld.GAME_2_TEAM_ID_SEASON: team_data[1][1]
            })
        else:
            aggregated.update({
                fld.GAME_2_HOME: np.nan,
                fld.GAME_2_TEAM_ID_SEASON: np.nan
            })
        fixtures_data.append(aggregated)
    return pd.DataFrame(fixtures_data)


def get_week_info(
        bootstrap_json: Dict[str, Any],
        api_version: Literal[1, 2],
        fixtures_json: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Read the file, extract info from json, denormalize into a dataframe
    """
    # Merge Info Player / Team / Position
    df_player = get_player_info(bootstrap_json)
    if api_version == 1:
        df_team = get_team_info_v1(bootstrap_json)
    else:
        df_team = get_team_info_v2(bootstrap_json, fixtures_json)
    df_position = get_position_info(bootstrap_json)

    logging.debug(df_player.transpose().head())
    logging.debug(df_team.transpose().head())
    logging.debug(df_position.transpose().head())

    # Add Gameweek + Season Info
    df_week = df_player.merge(df_team, on=fld.TEAM_ID).merge(df_position, on=fld.POSITION_ID)
    season_name = extract_season_name(bootstrap_json)
    prev_gw = extract_prev_gameweek(bootstrap_json)
    df_week[fld.GW_PREV] = prev_gw
    df_week[fld.GW] = get_next_game(prev_gw, season_name)
    df_week[fld.SEASON_NAME] = season_name
    df_week[fld.SEASON_ID] = int(season_name[:4]) - 2006 + 1

    return df_week


def preprocess_dataset(dir_bootstrap: Path, dir_fixtures: Optional[Path]) -> pd.DataFrame:
    """ Scan through all files in the raw data folder and turn it into a CSV"""
    # Scan Fixtures
    last_fix_season_file = {}
    logger.info(f'Screen folder {dir_fixtures}')
    for file_path in dir_fixtures.glob('*.json'):
        if file_path.is_file():
            with file_path.open(encoding='utf-8') as f_in:
                fixture_json = json.load(f_in)
                prev_gw = extract_fixture_prev_gameweek(fixture_json)
                season_name = extract_fixture_season(fixture_json)
                # Add the fixture path associated with the last gameweek for each season
                if season_name in last_fix_season_file:
                    if prev_gw > last_fix_season_file[season_name][1]:
                        last_fix_season_file[season_name] = (file_path, prev_gw)
                else:
                    last_fix_season_file[season_name] = (file_path, prev_gw)
    fixture_paths = {k: v[0] for k, v in last_fix_season_file.items()}
    logger.info(fixture_paths)

    # Scan Bootstraps
    last_bootstrap_gw_file = {}
    logger.info(f'Screen folder {dir_bootstrap}')
    for file_path in sorted(dir_bootstrap.glob('*.json')):
        if file_path.is_file():
            logger.debug(f'Screen file {file_path.name}')
            with file_path.open(encoding='utf-8') as f_in:
                bootstrap_json = json.load(f_in)
                season_gw = extract_season_name(bootstrap_json) + '_' + str(extract_prev_gameweek(bootstrap_json))
                last_bootstrap_gw_file[season_gw] = file_path
                logger.debug(f'Season gameweek: {season_gw}')

    # Extract data
    list_df = []
    for bootstrap_path in last_bootstrap_gw_file.values():
        logger.info(f'Preprocessing file {bootstrap_path.name}')
        with bootstrap_path.open(encoding='utf-8') as f_in:
            bootstrap_json = json.load(f_in)
            # Extract data (old API)
            if 'current-event' in bootstrap_json:
                df_week = get_week_info(bootstrap_json, api_version=1)
            # Extract data (new API)
            else:
                season = extract_season_name(bootstrap_json)
                season_fixture_path = fixture_paths[season]
                with season_fixture_path.open(encoding='utf-8') as fix_in:
                    fixture_json = json.load(fix_in)
                df_week = get_week_info(bootstrap_json, api_version=2, fixtures_json=fixture_json)
            # Append data
            list_df.append(df_week)

    df_bootstrap = pd.concat(list_df)
    df_bootstrap.drop_duplicates(inplace=True)

    # Append the result  (requires to merge on the previous gamweek)
    df_result = df_bootstrap.copy()[[fld.SEASON_ID, fld.GW_PREV, fld.PLAYER_ID, fld.RESULT_POINTS_PREV]]
    df_result.rename(columns={fld.RESULT_POINTS_PREV: fld.RESULT_POINTS, fld.GW_PREV: fld.GW},
                     inplace=True)

    df_merged = df_bootstrap.merge(df_result,
                                   how='left',
                                   on=[fld.SEASON_ID, fld.GW, fld.PLAYER_ID]
                                   )

    # Clean
    # Remove empty gameweeks (end of season)
    df_c = df_merged.dropna(subset=[fld.GW])
    # Remove empty gameweeks due to covid
    df_c = df_c[~((df_c[fld.GW_PREV].between(30, 38)) & (df_c[fld.SEASON_NAME] == '2019/20'))]
    # Fill empty fields
    df_c[fld.PLAYER_CHANCE_PLAY] = df_c[fld.PLAYER_CHANCE_PLAY].fillna(100)
    df_c[fld.RESULT_POINTS_PREV] = np.where(df_c[fld.GW] == 1, np.nan, df_c[fld.RESULT_POINTS_PREV])
    df_c[fld.STAT_MINUTES_TOTAL_SEASON] = np.where(df_c[fld.GW] == 1, np.nan, df_c[fld.STAT_MINUTES_TOTAL_SEASON])

    return df_c


def extract_season_name(bootstrap_json: Dict[str, Any]) -> str:
    events = bootstrap_json['events']
    year_start = events[0]['deadline_time'][:4]
    year_end = events[-1]['deadline_time'][:4]
    season_name = f'{year_start}/{year_end[2:]}'
    return season_name


def extract_prev_gameweek(bootstrap_json: Dict[str, Any]) -> Optional[int]:
    events = bootstrap_json['events']
    for event in events:
        if event['is_current']:
            return event['id']
    return None


def extract_fixture_prev_gameweek(fixture_json: Dict[str, Any]) -> Optional[int]:
    df = pd.DataFrame(fixture_json)
    last_game_played = df[df['started'].astype('bool')].groupby('event').size().index.max()
    return last_game_played


def extract_fixture_season(fixture_json: Dict[str, Any]) -> str:
    df = pd.DataFrame(fixture_json)
    df_dates = df['kickoff_time'].dropna()
    year_start = df_dates.str.slice(stop=4).min()
    year_end = df_dates.str.slice(stop=4).max()
    season_name = f'{year_start}/{year_end[2:]}'
    return season_name


def get_next_game(gw: int, season_name: str) -> Optional[int]:
    # covid year
    if season_name == '2019/20':
        if gw is not None:
            if 29 <= gw <= 38:
                next_gw = 39
            elif gw == 47:
                next_gw = None
            else:
                next_gw = gw + 1
        else:
            next_gw = 1
    # Normal year
    else:
        if gw is not None:
            if gw == 38:
                next_gw = None
            else:
                next_gw = gw + 1
        else:
            next_gw = 1
    return next_gw


def run():
    df_bootstrap = preprocess_dataset(struc.DIR_RAW_BOOTSTRAP, struc.DIR_RAW_FIXTURES)
    df_bootstrap.to_csv(struc.FILE_INTER_BOOTSTRAP, index=False, encoding='utf-8')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()

