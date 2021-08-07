import json
from pathlib import Path
from pipeline.transform.preprocess_bootstrap import get_week_info, extract_gameweek, get_player_info
import pandas as pd

from utils import assert_df_csv

pd.options.display.max_columns = 20
pd.options.display.width = 300

TEST_RESOURCES = Path(__file__).parent / 'test-resources'


def test_get_player_info_2020():
    df = get_player_info(json.load(TEST_RESOURCES.joinpath('bootstrap-static_2020.json').open(encoding='utf-8')))
    assert_df_csv(df, TEST_RESOURCES.joinpath('bootstrap_player_info_2020.csv'))


def test_get_player_info_2017():
    df = get_player_info(json.load(TEST_RESOURCES.joinpath('bootstrap-static_2017.json').open(encoding='utf-8')))
    assert_df_csv(df, TEST_RESOURCES.joinpath('bootstrap_player_info_2017.csv'))


def test_print():
    print(get_week_info(TEST_RESOURCES / 'bootstrap-static_2017.json'))
    print(get_week_info(TEST_RESOURCES / 'bootstrap-static_2020.json'))

# past points and minutes initialised at 0
# Check no empty "chance of playing"
# check points diff betzeen last and current event


def test_extract_gameweek():
    # example_boostrap = TEST_RESOURCES / 'bootstrap-static_2020.json'
    # with example_boostrap.open(encoding='utf-8') as f_in:
    #     bootstrap_data = json.load(f_in)
    # _extract_gameweek(bootstrap_data)
    test_dir = Path(__file__).parent.parent.parent.parent / 'data' / 'raw' / 'bootstrap'
    print(test_dir)
    all_files = list(test_dir.glob('2020*.json'))
    print(all_files)
    for path in all_files:
        with path.open(encoding='utf-8') as f_in:
            bootstrap_data = json.load(f_in)
            season, gw = extract_gameweek(bootstrap_data)
            print(f'{season}\t{gw}\t {path.name}')
