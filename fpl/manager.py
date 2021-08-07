import logging
from typing import List
from pipeline.download.scrape import scrape_manager_team
from manager_player import ManagerPlayer, PlayerRole

logger = logging.getLogger(__name__)


class ManagerTeam(object):
    """ Team of the manager in the FPL website """
    money_bank: int 
    money_total: int
    free_transfers: int
    players: List[ManagerPlayer]
    unlimited_transfers: bool

    def __init__(self, money: int = 1000, unlimited_transfers: bool = False):
        self.money_bank = money
        self.money_total = money
        self.free_transfers = 15
        self.players = []

        # Options
        self.unlimited_transfers = unlimited_transfers

    def fetch_fpl_info(self, email, password):
        manager_info = scrape_manager_team(email=email, password=password)
        self.money_bank = manager_info['helper']['bank']
        self.free_transfers = manager_info['helper']['transfers_state']['free']

        # Get players
        self.players = []
        for pick in manager_info['picks']:
            current_player = ManagerPlayer(id_season=pick['element'],
                                           role=self._get_role(pick),
                                           purchase_price=pick['purchase_price'],
                                           selling_price=pick['selling_price'])
            self.players.append(current_player)

        # Calculate total value
        self.money_total = sum([player.selling_price for player in self.players]) + self.money_bank

    @staticmethod
    def _get_role(pick: dict) -> PlayerRole:
        """ 
        Extract the position from the fpl api json
        It can be found in 3 fields of the "pick" elemenent
        * position : 12 to 15 are assigned to benched players
        * is_captain : boolean
        * is_vice_captain : boolean
        """
        role = PlayerRole.NORMAL
        if pick['position'] > 11:
            role = PlayerRole.BENCH
        if pick['is_vice_captain']:
            role = PlayerRole.VICE_CAPTAIN
        if pick['is_captain']:
            role = PlayerRole.CAPTAIN
        return role


