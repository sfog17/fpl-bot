from dataclasses import dataclass
from enum import Enum

class PlayerRole(Enum):
    """ Role of the football player in the FPL gameweek """
    BENCH = 0
    NORMAL = 1
    CAPTAIN = 2
    VICE_CAPTAIN = 3

@dataclass
class ManagerPlayer(object):
    """ Football Player that can be selected by the manager """
    id_season: int
    role: PlayerRole
    purchase_price: float
    selling_price: float