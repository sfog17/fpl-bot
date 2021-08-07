import json
from pathlib import Path
from pipeline.transform.preprocess_bootstrap import _get_week_info, _extract_gameweek
import pandas as pd
pd.options.display.max_columns = 20
pd.options.display.width = 300

TEST_RESOURCES = Path(__file__).parent.parent.parent / 'test-resources'


def test_print():
    print(_get_week_info(TEST_RESOURCES / '2017_bootstrap-static.json'))
    print(_get_week_info(TEST_RESOURCES / '2020_bootstrap-static.json'))

# past points and minutes initialised at 0
# Check no empty "chance of playing"
# check points diff betzeen last and current event


def test_extract_gameweek():
    # example_boostrap = TEST_RESOURCES / '2020_bootstrap-static.json'
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
            season, gw = _extract_gameweek(bootstrap_data)
            print(f'{season}\t{gw}\t {path.name}')
