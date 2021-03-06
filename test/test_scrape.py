import json
from pathlib import Path
from fpl.download_data.scrape import scrape_bootstrap, scrape_fixtures, scrape_user_history, _extract_season_name


TEST_RESOURCES = Path(__file__).parent / 'test-resources'


def test_scrape_bootstrap():
    filepath = scrape_bootstrap(Path('test-output'))
    assert filepath.exists()


def test_scrape_fixtures():
    filepath = scrape_fixtures(Path('test-output'))
    assert filepath.exists()


def test_scrape_user_history():
    file_paths = scrape_user_history(output_dir=Path('test-output'), nb_users=10, include_current=True)
    assert len(file_paths) == 2
    assert file_paths[0].exists()
    assert file_paths[1].exists()


def test_scrape_user_history_past_only():
    file_paths = scrape_user_history(output_dir=Path('test-output'), nb_users=10, include_current=False)
    assert len(file_paths) == 1
    assert file_paths[0].exists()


def test_get_season_name():
    example_boostrap = TEST_RESOURCES / 'example_bootstrap-static.json'
    with example_boostrap.open(encoding='utf-8') as f_in:
        bootstrap_data = json.load(f_in)
    assert _extract_season_name(bootstrap_data) == '2017/18'
