import numpy as np
import pandas as pd
import constants.fields as fld
from typing import List, Dict
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, LpSolverDefault, LpStatus
from user_player import PlayerRole

# Added during preprocess
SQUAD_ROLE = 'squad_role'
IN_USER_TEAM = 'in_user_team'


def preprocess(df_pred, col_pred):
    """
    Preprocess for linear optimisation - include captain
    double the lines - 1 for normal, 1 for captain
    """
    df_pred = df_pred.reset_index(drop=True)
    df_pred[SQUAD_ROLE] = PlayerRole.NORMAL

    df_pred_captain = df_pred.copy()
    df_pred_captain[SQUAD_ROLE] = PlayerRole.CAPTAIN

    # TODO replace with extra column 'Multiplier' - cleaner
    df_pred_captain[col_pred] = 2 * df_pred[col_pred]
    df_preprocessed = pd.concat([df_pred, df_pred_captain], axis=0).reset_index(drop=True)

    return df_preprocessed


def select_team(df_predict: pd.DataFrame,
                col_id: str = fld.PLAYER_ID,
                col_cost: str = fld.PLAYER_COST,
                col_pred: str = fld.ML_PREDICT,
                col_pos: str = fld.PLAYER_POSITION,
                col_team: str = fld.TEAM_NAME,
                value_team: int = 1000,
                free_transfers: int = 15,
                existing_team: List[int] = None
                ) -> List[Dict[int, PlayerRole]]:
    """ Optimise to find the best team for a gameweek
    Returns the list of the players in the new team and their role
    """

    # Pre-process (double the lines - 1 for normal, 1 for captain)
    df = preprocess(df_predict, col_pred=col_pred)
    if not existing_team:
        df[IN_USER_TEAM] = 0
    else:
        df[IN_USER_TEAM] = np.where(df[col_id].isin(existing_team), 1, 0)

    # SET HIGH LEVEL CONSTRAINTS
    max_players = 15
    max_cost = value_team
    nb_gkp = 2
    nb_def = 5
    nb_mid = 5
    nb_fwd = 3
    max_by_team = 3
    free_transfers = free_transfers

    # INIT LP SOLVER
    prob = LpProblem('TeamSelection', LpMaximize)

    # SOLVER SETUP
    player = df.index
    code = df[col_id]
    cost = df[col_cost]
    points = df[col_pred]
    position = df[col_pos]
    team = df[col_team]
    role = df[SQUAD_ROLE]
    in_user_team = df[IN_USER_TEAM]

    # x: binary variable for the selection of the player (0: not selected, 1: selected)
    x = LpVariable.dicts('Selected', player, cat='Binary')

    # SET THE OBJECTIVE FUNCTION
    prob += sum([x[p] * points[p] for p in player]), 'Total_Points'

    # SET THE CONSTRAINTS
    prob += lpSum([x[p] for p in player]) <= max_players
    prob += lpSum([x[p] * cost[p] for p in player]) <= max_cost
    prob += 15 - lpSum([x[p] * in_user_team[p] for p in player]) <= free_transfers

    # constraints position
    prob += lpSum([x[p] if position[p] == 'GKP' else 0 for p in player]) == nb_gkp
    prob += lpSum([x[p] if position[p] == 'DEF' else 0 for p in player]) == nb_def
    prob += lpSum([x[p] if position[p] == 'MID' else 0 for p in player]) == nb_mid
    prob += lpSum([x[p] if position[p] == 'FWD' else 0 for p in player]) == nb_fwd

    # contraints team
    for t in team.unique():
        prob += lpSum([x[p] if team[p] == t else 0 for p in player]) <= max_by_team

    # constraints captain
    prob += lpSum([x[p] if role[p] == PlayerRole.CAPTAIN else 0 for p in player]) == 1

    # constrains player_code (make sure that captain is not also selected as a normal player)
    for player_code in code.unique():
        prob += lpSum([x[p] if code[p] == player_code else 0 for p in player]) <= 1

    # WRAP UP AND SOLVE
    LpSolverDefault.msg = 0
    # prob.writeLP('TeamOpt.lp')
    prob.solve()

    # CHECK SOLUTION
    total_impact = prob.objective.value()
    status = LpStatus[prob.status]
    print('status:', status)
    print('Total Points:', total_impact)

    # RETRIEVE SOLUTION
    selection = pd.Series({p: x[p].varValue for p in player})
    df['selected'] = selection.astype('int64')

    selection = df[(df['selected'] == 1)]
    print(selection[[fld.PLAYER_ID, fld.PLAYER_COST, fld.ML_PREDICT, fld.PLAYER_POSITION, fld.TEAM_NAME, SQUAD_ROLE]])
    return selection
