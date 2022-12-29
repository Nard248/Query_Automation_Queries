from os.path import isdir
import datetime
import os


today_date = datetime.datetime.today()
year_name = today_date.year
month_digit = today_date.month


starting_path = "Y://Retention//All//Retention All//"
inbox_path = rf"X://{year_name }//{month_digit}//"

if not isdir(starting_path):
    starting_path = "Z://Retention//All//Retention All//"
    if not isdir(starting_path):
        starting_path = "//srvm-totofs//TTKC//Retention//All//Retention All//"


if not isdir(inbox_path):
    inbox_path = fr'//nl-gen-fs//Customer Acquisition and Retention//RETENTION//Arxiv Lists//Retention Lists Results//{year_name }//{month_digit}//'



black_list_path =           f"{starting_path}Blacklist, Players//Blacklist.xlsx"
adminka_file_path =         f'{starting_path}00Bonus//Casino//Done//{year_name}//{month_digit}//'
dropdown_data =             f"{starting_path}with_python//data//"
user_categories_data =      f"{starting_path}with_python//data//UserCategories.csv"
attachements_path =         f"{starting_path}with_python//Bonus_from_PanduncFiles//Attachements"
bonus_excel_path =          f'{starting_path}with_python//Bonus_from_PanduncFiles//Bonuses.xlsx'
cas_players_path =          f"{starting_path}Blacklist, Players//Players Casino.xlsx"
cas_new_players_path =      f"{starting_path}Blacklist, Players//Players.csv"
sport_players_path =        f"{starting_path}Blacklist, Players//Players Sport.xlsx"
daily_path =                f'{starting_path}Daily List//Daily List Query.csv'
spin_path =                 f"{starting_path}Free_Spin_Bonus.xlsx"
hasmik_path =               f"{starting_path}Daily List//Targeted Marketing//"
p2p_path =                  f'{starting_path}with_python//P2P//Ակցիաներ'
esports_path =              f'{starting_path}with_python//E_Sports//Ակցիաներ'
bog_path =                  f'{starting_path}with_python//BOG//Ակցիաներ'
be_file_path =              f'{starting_path}00Bonus//Sport//'
# hasmik_registration_path =  f"{starting_path}with_python//Registartion bonus//For_Hasmik//"
sport_path =                f'{starting_path}with_python//Sport//Ակցիաներ'
today_daily_path =          f"{starting_path}Daily List//"
vip_id_path =               f'{starting_path}/VIP.xlsx'
criteria_path =             f"{starting_path}with_python//Registartion bonus//Criteria.csv"
registration_camp_path =    f"{starting_path}with_python//Registration_campaigns//Ակցիաներ"
analytics_path = r'\\srvm-totofs\TTKC\Retention\Smbat Harutyunyan\Analytics\\'
daily_lists =               f'{starting_path}//Daily List//Daily//'


balance_codes = {'real':2,
                'bonus':15}