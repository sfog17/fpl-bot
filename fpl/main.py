import logging
import fpl.collect.download
import fpl.extract.bootstrap
import fpl.extract.historical
import fpl.transform.features
import fpl.model.predict
from fpl.user import UserTeam
from fpl.credentials import EMAIL, PASSWORD


def get_current_user_info():
    team = UserTeam(money=100)
    team.fetch_fpl_info(email=EMAIL, password=PASSWORD)
    print(team.money_bank)
    print(team.money_total)
    print(team.free_transfers)
    print(team.players)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # fpl.collect.download.get_historical('fpl_api_historical')
    # fpl.collect.download.get_rankings('rankings_test', 10)
    # fpl.extract.historical.run()
    # fpl.transform.features.run(reload_data=False)
    # fpl.model.predict.run(reload_data=True)
    # fpl.collect.download.get_user_info(email=EMAIL, password=PASSWORD)
    get_current_user_info()
