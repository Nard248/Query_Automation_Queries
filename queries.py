def get_deposit(query_start_date, query_end_date, min_dep_amount, count):
    query = f"""
    SELECT u.Base_UserID, 
        p.Amount Dep_amount_{count},
        p.create_date Date
    FROM Payment p
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u 
            ON u.UserID = p.UserID
        INNER JOIN C_PaymentSystem sp 
            ON sp.PaymentSystemId = p.PaymentSystemID
    WHERE p.modify_date >= '{query_start_date}'
        AND p.modify_date <= '{query_end_date}'
        AND u.PartnerID = 237
        AND p.SourceID = 2
        AND p.PaymentTypeID = 2
        and p.PaymentStatusID = 8    
        AND sp.PaymentSystemName not like '%transfer%'
        AND p.EmployeeUserID is null
        AND p.Amount >= {min_dep_amount}
    ORDER BY u.Base_UserID,
        p.create_date
    """
    return query


def get_p2p_bets(query_start_date, query_end_date, count):
    bet_qur = f'''
    SELECT u.Base_UserID,
        o.OrderDate,
        sum(o.OrderAmount) Bet_amount_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u 
            ON u.UserID = o.UserID
        INNER JOIN C_Game cg 
            ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc 
            ON cgc.GameCategoryID=cg.GameCategoryID
    WHERE cgc.GameCategoryName like '%p2p%'
        AND o.OrderDate >= '{query_start_date}'
        AND o.OrderDate <= '{query_end_date}'
        AND u.UserTypeID NOT IN (1,3,20,21)
        AND o.OrderStateID IN (2,3)
    GROUP BY u.Base_UserID,
        o.OrderDate
    '''
    return bet_qur


def get_bets(query_start_date, query_end_date, count):
    bet_qur = f'''
    SELECT u.Base_UserID,
        o.OrderDate,
        sum(o.OrderAmount) Bet_amount_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u 
            ON u.UserID = o.UserID
        INNER JOIN C_Game cg 
            ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc 
            ON cgc.GameCategoryID=cg.GameCategoryID
    WHERE o.OrderDate >= '{query_start_date}'
        AND o.OrderDate <= '{query_end_date}'
        AND u.UserTypeID NOT IN (1,3,20,21)
        AND o.OrderStateID IN (2,3)
    GROUP BY u.Base_UserID,
        o.OrderDate
    '''
    return bet_qur


def lost_bet_spec_game(query_start_date, query_end_date, percent, game_name, min_bet_amount, count):
    bet_qur = f"""
    SELECT u.Base_UserID,
        sum(o.OrderAmount) * {percent} Bonus_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u 
            ON u.UserID = o.UserID
        INNER JOIN C_Game cg 
            ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc 
            ON cgc.GameCategoryID=cg.GameCategoryID
    WHERE 
        (cg.Name_en LIKE '%{game_name}%' OR cgc.GameCategoryName LIKE '%{game_name}%')
        AND o.OrderDate >= '{query_start_date}' 
        AND o.OrderDate <= '{query_end_date}'
        AND o.OrderAmount >= {min_bet_amount}
        AND o.OrderStateID = 3
        AND u.UserTypeID NOT IN (1,3,20,21)
    GROUP BY u.Base_UserID
    """
    return bet_qur


def lost_bet_spec_game_min_lostbet(query_start_date, query_end_date, percent, game_name, min_lostbet, count):
    if type(game_name) == str:
        print('Calculating min_lostbet for special game')
        part = f"""(cg.Name_en like '%{game_name}%'
        or cgc.GameCategoryName like '%{game_name}%') and """
    else:
        part = ''

    bet_qur = f"""
    SELECT u.Base_UserID,
        sum(o.OrderAmount) * {percent} Bonus_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u 
            ON u.UserID = o.UserID
        INNER JOIN C_Game cg 
            ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc 
            ON cgc.GameCategoryID = cg.GameCategoryID
    WHERE o.OrderDate >= '{query_start_date}' 
        AND {part}
        AND o.OrderDate <= '{query_end_date}'
        AND cgc.GameCategoryName LIKE '%p2p%'
        AND o.OrderStateID = 3
        AND u.UserTypeID NOT IN (1,3,20,21)
    GROUP BY 
        u.Base_UserID
    HAVING
        sum(o.OrderAmount) >= {min_lostbet}
    """
    return bet_qur


def p2p_lostbet(query_start_date, query_end_date, percent, count):
    bet_qur = f'''
    SELECT u.Base_UserID,
        sum(o.OrderAmount) * {percent} Bonus_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u ON u.UserID = o.UserID
        INNER JOIN C_Game cg ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc on cgc.GameCategoryID = cg.GameCategoryID
    WHERE cgc.GameCategoryName LIKE '%p2p%'
        AND o.OrderDate >= '{query_start_date}'
        AND o.OrderDate <= '{query_end_date}'
        AND u.UserTypeID NOT IN (1,3,20,21)
        AND o.OrderStateID = 3
    GROUP BY u.Base_UserID
    '''
    return bet_qur


def p2p_winbet(query_start_date, query_end_date, percent, min_bet_amount, count):
    bet_qur = f'''
    SELECT u.Base_UserID,
        sum(o.OrderAmount) * {percent} Bonus_{count}
    FROM casino.orders o
        INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u ON u.UserID = o.UserID
        INNER JOIN C_Game cg ON cg.GameID = o.GameID
        INNER JOIN C_GameCategory cgc ON cgc.GameCategoryID = cg.GameCategoryID
    WHERE cgc.GameCategoryName LIKE '%p2p%'
        AND o.OrderDate >= '{query_start_date}'
        AND o.OrderDate <= '{query_end_date}'
        AND u.UserTypeID NOT IN (1,3,20,21)
        AND o.OrderStateID = 2
        AND o.OrderAmount >= {min_bet_amount}
    GROUP BY u.Base_UserID
    '''
    return bet_qur
