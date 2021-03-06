import logging
import pandas as pd
import fpl.constants.fields as fld
import fpl.download_data.scrape
import fpl.extract.bootstrap
import fpl.extract.historical
import fpl.transform.features
import fpl.model.predict
import fpl.optimise
from fpl.user import UserTeam
from fpl.credentials import EMAIL, PASSWORD

pd.options.display.max_columns = 15


def get_current_user_info():
    team = UserTeam(money=100)
    team.fetch_fpl_info(email=EMAIL, password=PASSWORD)
    print(team.money_bank)
    print(team.money_total)
    print(team.free_transfers)
    print(team.players)


def select_weekly_players():
    df = fpl.model.predict.run(reload_data=False)
    # Select only last gameweek

    df_season = df[df[fld.SEASON_ID] == df[fld.SEASON_ID].max()]
    df_gw = df_season[df_season[fld.GW] == df_season[fld.GW].max()]
    logging.info(f'Process Season {df_gw[fld.SEASON_NAME].unique()} - Week {df_gw[fld.GW].unique()}')
    print(fpl.optimise.select_team(df_gw))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # fpl.extract.player-details.run()
    # fpl.transform.features.run(reload_data=False)
    # fpl.model.predict.run(reload_data=True)
    # fpl.download_data.download.get_user_info(email=EMAIL, password=PASSWORD)
    # get_current_user_info()
    # select_weekly_players()
