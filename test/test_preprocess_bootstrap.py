import json
from pathlib import Path
import pipeline.transform.preprocess_bootstrap as pb
import pandas as pd

from utils import assert_df_csv_no_index

pd.options.display.max_columns = 20
pd.options.display.width = 300


TEST_RESOURCES = Path(__file__).parent / 'test-resources'
# Test 2017/18
BOOTSTRAP_2017_18 = TEST_RESOURCES.joinpath('20180101-bootstrap-static.json')
# Test 2019/20
BOOTSTRAP_2019_20_COVID = TEST_RESOURCES.joinpath('20210314_bootstrap_static.json')
FIXTURES_2019_20_COVID = TEST_RESOURCES.joinpath('20200801_fixtures.json')
# Test 2020/21
BOOTSTRAP_2020_21 = TEST_RESOURCES.joinpath('20210114_bootstrap_static.json')
BOOTSTRAP_2020_21_END = TEST_RESOURCES.joinpath('20210601_bootstrap_static.json')
FIXTURES_2020_21 = TEST_RESOURCES.joinpath('20210101_fixtures.json')
# Test 2021/22
BOOTSTRAP_2021_22_START = TEST_RESOURCES.joinpath('20210701_bootstrap_static.json')


def test_extract_season_name_2017_18():
    season = pb.extract_season_name(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')))
    assert season == '2017/18'


def test_extract_season_name_2020_21():
    season = pb.extract_season_name(json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')))
    assert season == '2020/21'


def test_extract_gameweek_2017_18():
    gw = pb.extract_prev_gameweek(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')))
    assert gw == 21


def test_extract_gameweek_2020_21():
    gw = pb.extract_prev_gameweek(json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')))
    assert gw == 18


def test_extract_gameweek_2021_22_start():
    gw = pb.extract_prev_gameweek(json.load(BOOTSTRAP_2021_22_START.open(encoding='utf-8')))
    assert gw is None


def test_get_player_info_2017_18():
    df = pb.get_player_info(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')))
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_player_info_2017_18.csv'))


def test_get_player_info_2020_21():
    df = pb.get_player_info(json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')))
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_player_info_2020_21.csv'))


def test_get_position_info_2017_18():
    df = pb.get_position_info(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')))
    df_expected = pd.DataFrame({'position_id': [1, 2, 3, 4], 'player_position': ['GKP', 'DEF', 'MID', 'FWD']})
    pd.testing.assert_frame_equal(df, df_expected)


def test_get_position_info_2020_21():
    df = pb.get_position_info(json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')))
    df_expected = pd.DataFrame({'position_id': [1, 2, 3, 4], 'player_position': ['GKP', 'DEF', 'MID', 'FWD']})
    pd.testing.assert_frame_equal(df, df_expected)


def test_get_team_info_2017_18():
    df = pb.get_team_info_v1(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')))
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_team_info_2017_18.csv'))


def test_get_fixture_info_2020_21():
    df = pb.get_fixtures_info(json.load(FIXTURES_2020_21.open(encoding='utf-8')), gameweek=19)
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_fixture_info_2020_21.csv'))


def test_extract_fixture_prev_gameweek():
    last_game_played = pb.extract_fixture_prev_gameweek(json.load(FIXTURES_2020_21.open(encoding='utf-8')))
    assert last_game_played == 16


def test_extract_fixture_season():
    season = pb.extract_fixture_season(json.load(FIXTURES_2020_21.open(encoding='utf-8')))
    assert season == '2020/21'


def test_get_team_info_2020_21_multiple_matches():
    df = pb.get_team_info_v2(
        json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')),
        json.load(FIXTURES_2020_21.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_team_info_2020_21.csv'))


def test_get_team_info_2021_22_single_match_only():
    df = pb.get_team_info_v2(
        json.load(BOOTSTRAP_2021_22_START.open(encoding='utf-8')),
        json.load(FIXTURES_2020_21.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_team_info_2021_22_start.csv'))


def test_get_team_info_2020_21_end():
    df = pb.get_team_info_v2(
        json.load(BOOTSTRAP_2020_21_END.open(encoding='utf-8')),
        json.load(FIXTURES_2020_21.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_team_info_2020_21_end.csv'))


def test_get_week_info_2017_18():
    df = pb.get_week_info(json.load(BOOTSTRAP_2017_18.open(encoding='utf-8')), api_version=1)
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_week_info_2017_18.csv'))


def test_get_week_info_2019_20_covid():
    df = pb.get_week_info(
        json.load(BOOTSTRAP_2019_20_COVID.open(encoding='utf-8')),
        api_version=2,
        fixtures_json=json.load(FIXTURES_2019_20_COVID.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_week_info_2019_20_covid.csv'))


def test_get_week_info_2020_21():
    df = pb.get_week_info(
        json.load(BOOTSTRAP_2020_21.open(encoding='utf-8')),
        api_version=2,
        fixtures_json=json.load(FIXTURES_2020_21.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_week_info_2020_21.csv'))


def test_get_week_info_2020_21_end():
    df = pb.get_week_info(
        json.load(BOOTSTRAP_2020_21_END.open(encoding='utf-8')),
        api_version=2,
        fixtures_json=json.load(FIXTURES_2020_21.open(encoding='utf-8'))
    )
    assert_df_csv_no_index(df, TEST_RESOURCES.joinpath('bootstrap_week_info_2020_21_end.csv'))
