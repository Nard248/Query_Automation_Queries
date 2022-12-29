def get_esports_bets(query_start_date, query_end_date, min_odd, count):
    bet_qur = f'''

    select 

        a.Base_UserID, 
        a.OrderDate,
        sum(a.Bet) Bet_amount_{count}


    from (
        SELECT 
            cu.Base_UserID, 
            o.CheckNumber, 
            o.OrderDate,
            sum(o.StakeAmount) Bet

        FROM 
            [dwOper].[dbo].[VIEW_sport_OrdersBetsStakes_TotogamingAm] o
            left join C_Game g on g.GameID = o.GameID
            left join VIEW_sport_PartnerUser_TotogamingAm u on u.UserID = o.UserID
            left JOIN VIEW_PlatformPartnerUsers_TotogamingAm cu on cu.PartnerUserId = u.PartnerUserId 
        where

            o.OrderDate >= '{query_start_date}'
            and o.OrderDate <= '{query_end_date}'
            and o.IsInternet = 1
            and StakeStateID in (2,3,4)
            and (
                g.GameID = 53 
                or g.Name_en like 'E-%'
                )
        group by 
            cu.Base_UserID,
            o.CheckNumber,
            o.OrderDate
        having 
            max(o.BetMaxWinAmount) / sum(o.StakeAmount) >= {min_odd}) a
    group by
        a.Base_UserID,
        a.OrderDate
    '''
    return bet_qur


def get_esport_ggr(query_start_date, query_end_date, min_odd, min_bet, percent, count, users=None):
    query = f"""
    SET NOCOUNT ON;
    drop table if exists #check_numbers
    SELECT
    o.CheckNumber
   into #check_numbers
    FROM
        [dwOper].[dbo].[VIEW_sport_OrdersBetsStakes_TotogamingAm] o
        left join VIEW_sport_PartnerUser_TotogamingAm u on u.UserID = o.UserID
        left JOIN VIEW_PlatformPartnerUsers_TotogamingAm cu on cu.PartnerUserId = u.PartnerUserId
        left join C_Game g on g.GameID = o.GameID



   where
        o.OrderDate >= '{query_start_date}'
        and o.OrderDate <= '{query_end_date}'
        and o.CalculationDate >= '{query_start_date}'
        and o.CalculationDate <= '{query_end_date}'
        and (
        g.GameID = 53
        or g.Name_en like 'E-%'
        )
        and o.IsInternet = 1
        --and cu.Base_UserID in {users}

select a.Base_UserID,sum(a.Bonus) Bonus_{count}
from 
(select cu.Base_UserID,
(sum(so.StakeAmount)- sum(so.winamount)) * {percent} Bonus
from VIEW_sport_OrdersBetsStakes_TotogamingAm so
left join VIEW_sport_PartnerUser_TotogamingAm u on u.UserID = so.UserID
left JOIN VIEW_PlatformPartnerUsers_TotogamingAm cu on cu.PartnerUserId = u.PartnerUserId
where so.CheckNumber in (select * from #check_numbers)
group by cu.Base_UserID,so.CheckNumber
having max(so.BetMaxWinAmount) / sum(so.StakeAmount) >= {min_odd}
    and sum(so.StakeAmount) >= {min_bet}
    and sum(so.StakeAmount)- sum(so.winamount)>0) a
GROUP by a.Base_UserID

    """

    return query