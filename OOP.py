from os.path import isdir

starting_path = "Y://Retention//All//Retention All//"

if not isdir(starting_path):
    starting_path = "Z://Retention//All//Retention All//"
    if not isdir(starting_path):
        starting_path = "//srvm-totofs//TTKC//Retention//All//Retention All//"
import sys

sys.path.append(f'{starting_path}with_python//')

import datetime
import pandas as pd

pd.options.mode.chained_assignment = None

today_date = datetime.datetime.today().date()
yesterday = today_date - datetime.timedelta(1)
this_year = yesterday.year
month_digit = yesterday.month
file_name_date = yesterday.strftime('%d.%m.%Y')

inbox_path = f'//nl-gen-fs//Customer Acquisition and Retention//RETENTION//Arxiv Lists//Retention Lists Results//{this_year}//{month_digit}//'

from paths import *


class P2P:
    data = pd.read_excel(f"{starting_path}with_python//P2P//p2p_data.xlsx")

    deposit_bonus = data[data['py_id'].str[:4] == 'D_01']['Bonus_ID']
    spec_game_lost_bet = data[data['py_id'].str[:4] == 'B_02']['Bonus_ID']
    spec_game_lost_bet_min_lostbet = data[data['py_id'].str[:4] == 'B_03']['Bonus_ID']
    p2p_lostbet = data[data['py_id'].str[:4] == 'B_04']['Bonus_ID']
    p2p_winbet = data[data['py_id'].str[:4] == 'B_05']['Bonus_ID']
    deposit_bonus_new_users = data[data['py_id'].str[:4] == 'D_06']['Bonus_ID']
    p2p_lost_bet_min_lostbet = data[data['py_id'].str[:4] == 'B_07']['Bonus_ID']
    all_types = (p2p_lost_bet_min_lostbet, deposit_bonus_new_users, p2p_winbet, deposit_bonus, spec_game_lost_bet,
                 spec_game_lost_bet_min_lostbet, p2p_lostbet)
    path = p2p_path

    def __init__(self, action, path=path, data=data):
        self.today_date = datetime.datetime.today().date()
        self.fin = data[data['Bonus_ID'] == action]

        self.start_date = self.fin.iloc[0]['Start date'].date()
        self.end_date = self.fin.iloc[0]['End date'].date()
        self.lasts = (self.end_date - self.start_date).days + 1
        self.count = (self.today_date - self.start_date).days
        self.campaign_name = self.fin.iloc[0]['Campaign name'].strip()
        self.bonus_type = self.fin.iloc[0]['py_id']
        self.game_name = self.fin.iloc[0]['Game name']
        self.percent = self.fin.iloc[0]['Percent'] / 100
        self.min_dep_amount = self.fin.iloc[0]['Min deposit']
        self.min_bet_amount = self.fin.iloc[0]['Min bet']
        self.min_lostbet = self.fin.iloc[0]['Min lost bet']
        self.max_bonus_amount = self.fin.iloc[0]['Max bonus']
        self.folder_date = self.start_date.strftime('%d.%m.%Y')
        self.folder_name = self.campaign_name + '_' + self.folder_date
        self.list_name = self.campaign_name.replace(' ', '_') + '_' + 'Sms_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_call = self.campaign_name.replace(' ', '_') + '_' + 'Call_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_mail = self.campaign_name.replace(' ', '_') + '_' + 'Mail_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.history_bonuses = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Bonuses//{self.campaign_name}_{self.folder_date}.csv"
        self.original_bonus = f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'
        self.original_list = f'{path}//{self.folder_name}//{self.list_name}'
        self.history_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//SMS//{self.list_name}"
        self.original_call_list = f'{path}//{self.folder_name}//{self.list_name_call}'
        self.history_call_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Call//{self.list_name_call}"
        self.original_mail_list = f'{path}//{self.folder_name}//{self.list_name_mail}'
        self.history_mail_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Mail//{self.list_name_mail}"
        self.lower_bound = 200
        self.key_column = 'CasinoID'

        self.users = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name}', usecols=[self.key_column])
        try:
            users2 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_call}', usecols=[self.key_column])
            self.users = pd.concat([self.users, users2], ignore_index=True)
        except:
            pass
        try:
            users3 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_mail}', usecols=[self.key_column])

            self.users = pd.concat([self.users, users3], ignore_index=True)
        except:
            pass
        self.users.drop_duplicates(subset=[self.key_column], inplace=True)
        self.users.dropna(subset=[self.key_column], inplace=True)

    def check_validity(self, path=path):
        if self.count > self.lasts:
            print(f'{self.campaign_name} ավարտվել է {self.end_date}-ին')

            if isfile(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'):
                move(self.original_bonus, self.history_bonuses)
                try:
                    move(self.original_list, self.history_lists)
                except:
                    pass
                try:
                    move(self.original_call_list, self.history_call_lists)
                except:
                    pass
                try:
                    move(self.original_mail_list, self.history_mail_lists)
                except:
                    pass
                rmtree(f'{path}//{self.folder_name}')
            return False
        elif self.count <= 0:
            print(f'{self.campaign_name} սկսվելու է {self.start_date}-ին')
            return False
        return True

    def get_data(self):
        while True:
            try:
                cnxn = connect(
                    "Driver={ODBC Driver 17 for SQL Server};"
                    "Server=DWH;"
                    "Database=dwOper;"
                    "Trusted_Connection=yes;"
                )
                break
            except:
                print("Trying again")
                continue
        if self.bonus_type[4] == '2' and self.bonus_type[-1] == '1':
            query_start_date = datetime.datetime.strftime(self.start_date - datetime.timedelta(1),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        else:

            query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                          format='%Y-%m-%d') + ' 20:00:00'

        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        if self.bonus_type[2:4] == '01':
            dep_q = queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = queries.get_p2p_bets(query_start_date, query_end_date, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets
        elif self.bonus_type[2:4] == '06':
            dep_q = queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = queries.get_bets(query_start_date, query_end_date, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets
        elif self.bonus_type[2:4] == '02':
            bet_q = queries.lost_bet_spec_game(query_start_date, query_end_date, self.percent, self.game_name,
                                               self.min_bet_amount, self.count)
        elif self.bonus_type[2:4] == '03':
            bet_q = queries.lost_bet_spec_game_min_lostbet(query_start_date, query_end_date, self.percent,
                                                           self.game_name, self.min_lostbet, self.count)
        elif self.bonus_type[2:4] == '04':
            bet_q = queries.p2p_lostbet(query_start_date, query_end_date, self.percent, self.count)
        elif self.bonus_type[2:4] == '05':
            bet_q = queries.p2p_winbet(query_start_date, query_end_date, self.percent, self.min_bet_amount, self.count)
        elif self.bonus_type[2:4] == '07':
            bet_q = queries.lost_bet_spec_game_min_lostbet(query_start_date, query_end_date, self.percent,
                                                           self.game_name, self.min_lostbet, self.count)
        # new bonus here
        else:
            raise Exception('Unknown bonus type:', self.bonus_type)
        bets = pd.read_sql(bet_q, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets, deposit=None):
        if self.bonus_type[4] == '2':  # դեպոզիտից ա թե չէ
            deposit.drop_duplicates('Base_UserID', inplace=True)
            bets.set_index('Base_UserID', inplace=True)
            joined = deposit.join(bets, 'Base_UserID')

            clean_bet = joined[joined['OrderDate'] >= joined['Date']][['Base_UserID', f'Bet_amount_{self.count}']]
            grouped_bets = clean_bet.groupby('Base_UserID').sum()
            all_in_one = deposit.join(grouped_bets, 'Base_UserID').drop('Date', axis=1)
            filtered_by_worth = all_in_one.loc[
                all_in_one[f'Bet_amount_{self.count}'] >= all_in_one[f'Dep_amount_{self.count}']]

            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(deposit):
                    if deposit >= 1000 and deposit <= 5000:
                        return '1000-5000'
                    elif deposit > 5000 and deposit <= 10000:
                        return '5001-10000'
                    elif deposit > 10000 and deposit <= 20000:
                        return '10001-20000'
                    elif deposit > 20000:
                        return 20001

                filtered_by_worth['Ranges'] = filtered_by_worth[f'Dep_amount_{self.count}'].apply(set_ranges)
                joined = filtered_by_worth.join(for_join, 'Ranges')
                joined[f'Bonus_amount_{self.count}'] = (joined[f'Dep_amount_{self.count}'] * joined['Percent']) / 100
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
            elif self.bonus_type[0] == 'D':
                filtered_by_worth[f'Bonus_amount_{self.count}'] = filtered_by_worth[
                                                                      f'Dep_amount_{self.count}'] * self.percent
                joined = filtered_by_worth.copy()

            joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                          upper=self.max_bonus_amount)
            joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
            joined.set_index('Base_UserID', inplace=True)

            all_in = self.users.join(joined, self.key_column)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            return all_in

        elif self.bonus_type[4] == '1':  # մենակ bet-ով ակցիա
            bets.set_index('Base_UserID', inplace=True)
            all_in = self.users.join(bets, self.key_column)
            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(bet):
                    if bet >= 1000 and bet <= 10000:
                        return '1000-10000'
                    elif bet > 10000 and bet <= 20000:
                        return '10001-20000'
                    elif bet > 20000 and bet <= 60000:
                        return '20001-60000'
                    elif bet > 60000:
                        return 60001

                all_in['Ranges'] = all_in[f'Bet_{self.count}'].apply(set_ranges)
                joined = all_in.join(for_join, 'Ranges')

                joined[f'Bonus_amount_{self.count}'] = (joined[f'Bet_{self.count}'] * joined['Percent']) / 100
                joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                              upper=self.max_bonus_amount)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].fillna(0)
                return joined
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(lower=self.lower_bound,
                                                                                   upper=self.max_bonus_amount)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
            return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            if self.bonus_type[-1] == '2':
                # print(f'{action} action gives bonuses twice')
                def adjusting(today, total):
                    past_days = total - today
                    if total > self.max_bonus_amount:
                        curent = self.max_bonus_amount - past_days
                        if curent < 0:
                            return 0
                        return curent
                    else:
                        return today

                as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                    lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
                as_last_day.drop('Total', inplace=True, axis=1)
                as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            elif self.bonus_type[-1] == '1':
                # print(f'"{action}" action gives bonuses once')
                as_last_day.loc[as_last_day['Total'] != 0, f'Adj_bonus_{self.count}'] = 0
                as_last_day.drop('Total', inplace=True, axis=1)
            else:
                raise Exception('No such type bonus: ', self.bonus_type)
            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]

        for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount', self.key_column: 'ClientID'}, inplace=True)
        for_adminka['Currency'] = 'AMD'
        for_adminka['BalanceType'] = 15
        for_adminka['OperationType'] = 57
        for_adminka['Info'] = self.campaign_name
        for_adminka["Generated Unique Id"] = None
        if isfile(f"{adminka_file_path}P2P_{file_name_date}.csv"):
            for_adminka.to_csv(f"{adminka_file_path}P2P_{file_name_date}.csv", index=False, mode='a', header=False)
        else:
            for_adminka.to_csv(f"{adminka_file_path}P2P_{file_name_date}.csv", index=False)
        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')


class Analytics:
    analytic_month_name = (datetime.datetime.today() - datetime.timedelta(30)).strftime('%B')
    analytic_month_digit = (datetime.datetime.today() - datetime.timedelta(30)).month
    analytic_month_year_digit = (datetime.datetime.today() - datetime.timedelta(30)).year
    sms_result_file = fr'{starting_path}with_python\Analytics\Joined_dexatel_{analytic_month_name}.csv'
    sms_result_path = f'{starting_path}SMS results//{this_year}//{analytic_month_digit}//'

    def __init__(self, product=None, coworker=None):
        if product:
            self.product = product
            self.db_path = f'{starting_path}with_python//{product}//Ակցիաներ//History//{Analytics.analytic_month_year_digit}//{Analytics.analytic_month_digit}//'
            self.bonus_files_path = f'{self.db_path}Bonuses//'
            self.lists_path = f'{self.db_path}Lists//SMS//'
            self.sms_result_destination = f'{self.db_path}Analytics//SMS_results//'
            self.bonus_destination = f'{self.db_path}Analytics//users_bonus//'
        elif coworker:
            self.lists_path = rf'{starting_path}with_python\Analytics\{coworker}\\'
            self.sms_result_destination = rf'{starting_path}with_python\Analytics\{coworker}_SMS_result\\'

    def merging_sms_results(self, dexatel_file_name=None, nikita_web=None, nikita_mark=None):
        print('Creating SMS unique file')
        if dexatel_file_name:
            dexatel = pd.read_excel(Analytics.sms_result_path + f"{dexatel_file_name}.xlsx", header=1)
            dexatel_filtered = dexatel[dexatel['Status'].isin(['DELIVRD', 'SENT'])][
                ['External label for pack of EDR', 'Recipient', 'Message text']]
            dexatel_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            dexatel_filtered = pd.DataFrame()

        if nikita_web:
            nikita_web = pd.read_excel(Analytics.sms_result_path + f"{nikita_web}.xlsx")
            nikita_web_filtered = nikita_web[nikita_web['Status'].isin(['Delivered', 'Transmitted'])][
                ['Group', 'MSISDN', 'Message']]
            nikita_web_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            nikita_web_filtered = pd.DataFrame()

        if nikita_mark:
            nikita_2 = pd.read_excel(Analytics.sms_result_path + f"{nikita_mark}.xlsx")
            nikita_2_filtered = nikita_2[nikita_2['Status'].isin(['Delivered', 'Transmitted'])][
                ['Group', 'MSISDN', 'Message']]
            nikita_2_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            nikita_2_filtered = pd.DataFrame()

        sms_result_new = pd.concat([dexatel_filtered, nikita_web_filtered, nikita_2_filtered], ignore_index=True)

        sms_result_new['Campaign ID'] = sms_result_new['Campaign ID'].str.strip()
        sms_result_new.to_csv(Analytics.sms_result_file, index=False)
        self.sms_result_new = sms_result_new

    def get_sms_data(self):
        print('Getting SMS data')
        if isfile(Analytics.sms_result_file):
            self.sms_result_new = pd.read_csv(Analytics.sms_result_file)
            return self.sms_result_new
        else:
            dexatel_file_name = input('Input dexatel file name: ')
            nikita_web = input('Input nikita web file name: ')
            nikita_mark = input('Input nikita mark file name: ')
            self.merging_sms_results(dexatel_file_name, nikita_web, nikita_mark)
            return pd.read_csv(Analytics.sms_result_file, dtype={'PhoneNumber': 'int'})

    def lists_with_sms_results(self):

        sms_files = os.listdir(self.lists_path)

        for file in sms_files:

            try:
                df = pd.read_excel(self.lists_path + file)
            except ValueError as v_err:
                df = pd.read_csv(self.lists_path + file)

            df.dropna(subset=['PartnerUserId'], inplace=True)

            try:
                campaign_id = df.iloc[0]['Campaign ID'].strip()  # .split('_')[0]
            except:
                print('Campaign ID is empty:', file)
                continue

            filtered_sms = self.sms_result_new[self.sms_result_new['Campaign ID'] == campaign_id][
                ['PhoneNumber', 'Message']]

            if filtered_sms.shape[0] == 0:
                print(f'Can not find {campaign_id} from {file} data in sms list')
                continue

            merged = pd.merge(df, filtered_sms, left_on='Contact ID (tel/mail)', right_on='PhoneNumber', how='left')

            column_na_count = merged['PhoneNumber'].isna().sum()
            if merged.shape[0] == column_na_count:
                print(f'{campaign_id} from {file} does not find anybody')
                continue

            merged.drop('Message', axis=1, inplace=True)
            merged['Customer comment'] = merged['PhoneNumber'].copy()
            merged.drop('PhoneNumber', axis=1, inplace=True)
            merged.to_excel(self.sms_result_destination + splitext(file)[0] + '.xlsx', index=False)

    def calculate_bonus(self):

        data = pd.read_excel(f"{starting_path}with_python//{self.product}//{self.product}_data.xlsx",
                             sheet_name=Analytics.analytic_month_name)

        if self.product == 'Sport':
            col_names = ['SportID', 'Total', 'ID']
        elif self.product == 'P2P':
            col_names = ['CasinoID', 'Total', 'ID']
        data.dropna(subset=['Campaign name'], inplace=True)

        for index, row in data.iterrows():

            bonus_id = row['Bonus_ID']
            #         if bonus_id in skip_action:
            #             continue
            file_name = row['Campaign name'].strip() + '_' + str(row['Start date'].date().strftime('%d.%m.%Y')) + '.csv'
            #         bonus_file = pd.read_csv(f'{raw_files_path}Bonuses//'+file_name,usecols=['CasinoID','Total'])
            bonus_file = pd.read_csv(f'{self.bonus_files_path}//' + file_name)
            adj_bonus_cols = [col for col in bonus_file.columns if 'Adj_bonus' in col]
            bonus_file['Total'] = bonus_file[adj_bonus_cols].sum(axis=1)
            without_0 = bonus_file[bonus_file['Total'] != 0]
            without_0['ID'] = bonus_id

            try:
                without_0 = without_0[col_names]
            except:
                col_names = ['CasinoID', 'Total', 'ID']
                without_0 = without_0[col_names]

            # without_0.rename(columns={'SportID':'CasinoID'})

            if index == 0:
                final_bonus_file = without_0
            else:
                final_bonus_file = pd.concat([final_bonus_file, without_0], ignore_index=True)

        final_bonus_file.to_csv(f'{self.bonus_destination}bonus.csv', index=False)


class E_Sports(P2P):
    data = pd.read_excel(f"{starting_path}with_python//E_Sports//Campaigns.xlsx")

    deposit_bonus = data[data['py_id'].str[:4] == 'D_01']['Bonus_ID'].values
    ggr_bonus = data[data['py_id'].str[:4] == 'B_02']['Bonus_ID'].values
    all_types = (deposit_bonus, ggr_bonus)

    path = esports_path

    def __init__(self, action, path=path, data=data):

        super().__init__(action, path, data)

        self.min_odd = self.fin.iloc[0]['Min odd']
        self.freespin_id = self.fin.iloc[0]['Freespin ID']
        self.one_time = self.fin.iloc[0]['One time']
        self.path = path
        if self.one_time:
            self.count = (self.today_date.date() - self.end_date).days
            self.lasts = 1

    def get_data(self):
        cnxn = connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DWH;"
            "Database=dwOper;"
            "Trusted_Connection=yes;"
        )
        if (self.bonus_type[4] == '2' and self.bonus_type[-1] == '1') or self.one_time:

            query_start_date = datetime.datetime.strftime(self.start_date - datetime.timedelta(1),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        else:
            query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        if self.bonus_type[2:4] == '01':
            dep_q = e_queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = e_queries.get_esports_bets(query_start_date, query_end_date, self.min_odd, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets

        elif self.bonus_type[2:4] == '02':

            bet_q = e_queries.get_esport_ggr(query_start_date, query_end_date, self.min_odd, self.min_bet_amount,
                                             self.percent, self.count)  # ,tuple(self.users[self.key_column].values)

        # new bonus here

        else:
            raise Exception('Unknown bonus type:', self.bonus_type)
        bets = pd.read_sql(bet_q, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets, deposit=None):
        if self.bonus_type[4] == '2':  # դեպոզիտից ա թե չէ
            deposit.drop_duplicates('Base_UserID', inplace=True)
            bets.set_index('Base_UserID', inplace=True)
            joined = deposit.join(bets, 'Base_UserID')

            clean_bet = joined[joined['OrderDate'] >= joined['Date']][['Base_UserID', f'Bet_amount_{self.count}']]
            grouped_bets = clean_bet.groupby('Base_UserID').sum()
            all_in_one = deposit.join(grouped_bets, 'Base_UserID').drop('Date', axis=1)
            filtered_by_worth = all_in_one.loc[
                all_in_one[f'Bet_amount_{self.count}'] >= all_in_one[f'Dep_amount_{self.count}']]

            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(deposit):
                    if deposit >= 1000 and deposit <= 5000:
                        return '1000-5000'
                    elif deposit > 5000 and deposit <= 10000:
                        return '5001-10000'
                    elif deposit > 10000 and deposit <= 20000:
                        return '10001-20000'
                    elif deposit > 20000:
                        return 20001

                filtered_by_worth['Ranges'] = filtered_by_worth[f'Dep_amount_{self.count}'].apply(set_ranges)
                joined = filtered_by_worth.join(for_join, 'Ranges')
                joined[f'Bonus_amount_{self.count}'] = (joined[f'Dep_amount_{self.count}'] * joined['Percent']) / 100
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
            elif self.bonus_type[0] == 'D':
                if self.freespin_id:
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = self.freespin_id
                else:
                    filtered_by_worth[f'Bonus_amount_{self.count}'] = filtered_by_worth[
                                                                          f'Dep_amount_{self.count}'] * self.percent
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = filtered_by_worth[f'Bonus_amount_{self.count}'].clip(
                        lower=self.lower_bound, upper=self.max_bonus_amount)
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = filtered_by_worth[f'Adj_bonus_{self.count}'].apply(
                        round)
                filtered_by_worth.set_index('Base_UserID', inplace=True)
                joined = filtered_by_worth.copy()

            all_in = self.users.join(joined, self.key_column)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            return all_in

        elif self.bonus_type[4] == '1':  # մենակ bet-ով ակցիա
            bets.set_index('Base_UserID', inplace=True)
            all_in = self.users.join(bets, self.key_column)
            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(bet):
                    if bet >= 1000 and bet <= 10000:
                        return '1000-10000'
                    elif bet > 10000 and bet <= 20000:
                        return '10001-20000'
                    elif bet > 20000 and bet <= 60000:
                        return '20001-60000'
                    elif bet > 60000:
                        return 60001

                all_in['Ranges'] = all_in[f'Bet_{self.count}'].apply(set_ranges)
                joined = all_in.join(for_join, 'Ranges')

                joined[f'Bonus_amount_{self.count}'] = (joined[f'Bet_{self.count}'] * joined['Percent']) / 100
                joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                              upper=self.max_bonus_amount)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].fillna(0)
                return joined
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(lower=self.lower_bound,
                                                                                   upper=self.max_bonus_amount)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
            return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            if self.bonus_type[-1] == '2':
                # print(f'{action} action gives bonuses twice')
                def adjusting(today, total):
                    past_days = total - today
                    if total > self.max_bonus_amount:
                        curent = self.max_bonus_amount - past_days
                        if curent < 0:
                            return 0
                        return curent
                    else:
                        return today

                as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                    lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
                as_last_day.drop('Total', inplace=True, axis=1)
                as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            elif self.bonus_type[-1] == '1':
                # print(f'"{action}" action gives bonuses once')
                as_last_day.loc[as_last_day['Total'] != 0, f'Adj_bonus_{self.count}'] = 0
                as_last_day.drop('Total', inplace=True, axis=1)
            else:
                raise Exception('No such type bonus: ', self.bonus_type)
            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
            for_adminka.rename(columns={self.key_column: 'ClientID'}, inplace=True)

        if self.freespin_id:
            for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'BonusId'}, inplace=True)
            for_adminka['Note'] = self.campaign_name

        else:

            for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount'}, inplace=True)
            for_adminka['Currency'] = 'AMD'
            for_adminka['BalanceType'] = 15
            for_adminka['OperationType'] = 57
            for_adminka['Info'] = self.campaign_name

        for_adminka["Generated Unique Id"] = None

        filename_to_be_saved = f"{adminka_file_path}E_Sports_{file_name_date}"
        if self.freespin_id:
            filename_to_be_saved += '_freespin'

        if isfile(filename_to_be_saved + ".csv"):
            mode = 'a'
            header = False
        else:
            mode = 'w'
            header = True

        for_adminka.to_csv(f"{filename_to_be_saved}.csv", index=False, mode=mode, header=header)
        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')


class BOG:
    path = bog_path

    def __init__(self, campaign_name, lower_bound, max_bonus_amount, path=path):

        self.start_date = datetime.datetime(2022, 9, 13)
        self.end_date = datetime.datetime(2022, 9, 15)
        self.lasts = (self.end_date - self.start_date).days + 1
        self.count = (self.today_date - self.start_date).days
        self.campaign_name = campaign_name
        self.percent = 100 / 100
        self.max_bonus_amount = max_bonus_amount
        self.folder_date = self.start_date.strftime('%d.%m.%Y')
        self.folder_name = self.campaign_name + '_' + self.folder_date
        self.list_name = self.campaign_name.replace(' ', '_') + '_' + 'Sms_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_call = self.campaign_name.replace(' ', '_') + '_' + 'Call_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_mail = self.campaign_name.replace(' ', '_') + '_' + 'Mail_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'

        # self.history_bonuses =f"{path}//History//{self.start_date.year}//{self.start_date.month}//Bonuses//{self.campaign_name}_{self.folder_date}.csv"
        self.original_bonus = f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'
        self.original_list = f'{path}//{self.folder_name}//{self.list_name}'
        # self.history_lists =  f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//SMS//{self.list_name}"
        self.original_call_list = f'{path}//{self.folder_name}//{self.list_name_call}'
        # self.history_call_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Call//{self.list_name_call}"
        self.original_mail_list = f'{path}//{self.folder_name}//{self.list_name_mail}'
        # self.history_mail_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Mail//{self.list_name_mail}"
        # self.lower_bound = lower_bound
        self.key_column = 'CasinoID'
        self.users = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name}', usecols=[self.key_column])

        try:
            users2 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_call}', usecols=[self.key_column])
            self.users = pd.concat([self.users, users2], ignore_index=True)
        except:
            pass
        try:
            users3 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_mail}', usecols=[self.key_column])

            self.users = pd.concat([self.users, users3], ignore_index=True)
        except:
            pass
        self.users.drop_duplicates(subset=[self.key_column], inplace=True)
        self.users.dropna(subset=[self.key_column], inplace=True)

    def get_data(self):
        cnxn = connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DWH;"
            "Database=dwOper;"
            "Trusted_Connection=yes;"
        )

        query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                      format='%Y-%m-%d') + ' 20:00:00'
        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        ggr_query = f"""
        SELECT u.Base_UserID
        , SUM(CASE 
                WHEN o.CalculationDate_DT < '2021-03-01'
                    THEN CASE 
                            WHEN cg.GameProviderID IN (48, 10)
                                AND o.TypeId = 1
                                AND o.OrderDate > '2020-05-20 06:45:00'
                                THEN o.OrderAmount * o.Odds / 100
                            ELSE (o.OrderAmount - o.WinAmount)
                            END
                ELSE CASE 
                        WHEN cg.GameProviderID IN (48, 10)
                            AND o.TypeId = 1
                            AND o.OrderDate > '2020-05-20 06:45:00'
                            THEN o.OrderAmount * o.Odds / 100
                        WHEN cg.GameProviderID IN (48, 10)
                            AND o.TypeId IN (5, 8, 18, 33)
                            THEN (o.OrderAmount - o.WinAmount)
                        WHEN cg.GameProviderID NOT IN (48, 10)
                            THEN (o.OrderAmount - o.WinAmount)
                        ELSE 0
                        END
                END) * {self.percent} Bonus_{self.count} 
    FROM casino.orders o
    INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u ON u.UserID = o.UserID
    INNER JOIN C_Game cg ON cg.GameID = o.GameID
    INNER JOIN C_GameProvider p ON p.GameProviderID= cg.GameProviderID


    WHERE u.UserTypeID <> 1
        AND o.OrderDate >= '{query_start_date}'
        AND o.OrderDate < '{query_end_date}'
        AND o.OrderStateID NOT IN (1, 4, 7)
        AND o.OperationTypeID = 3
        AND CASE 
            WHEN cg.GameProviderID IN (48, 10)
                AND o.CalculationDate_DT < '2021-03-01'
                THEN o.TypeId
            ELSE 0
            END IN (0, 1, 5, 8, 18, 33)
        and p.GameProviderID = 25
    GROUP BY u.Base_UserID
    """
        # having SUM(CASE
        #             WHEN o.CalculationDate_DT < '2021-03-01'
        #                 THEN CASE
        #                         WHEN cg.GameProviderID IN (48, 10)
        #                             AND o.TypeId = 1
        #                             AND o.OrderDate > '2020-05-20 06:45:00'
        #                             THEN o.OrderAmount * o.Odds / 100
        #                         ELSE (o.OrderAmount - o.WinAmount)
        #                         END
        #             ELSE CASE
        #                     WHEN cg.GameProviderID IN (48, 10)
        #                         AND o.TypeId = 1
        #                         AND o.OrderDate > '2020-05-20 06:45:00'
        #                         THEN o.OrderAmount * o.Odds / 100
        #                     WHEN cg.GameProviderID IN (48, 10)
        #                         AND o.TypeId IN (5, 8, 18, 33)
        #                         THEN (o.OrderAmount - o.WinAmount)
        #                     WHEN cg.GameProviderID NOT IN (48, 10)
        #                         THEN (o.OrderAmount - o.WinAmount)
        #                     ELSE 0
        #                     END
        #             END) * {self.percent} > {self.lower_bound}

        bets = pd.read_sql(ggr_query, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets):

        bets.set_index('Base_UserID', inplace=True)
        all_in = self.users.join(bets, self.key_column)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(upper=self.max_bonus_amount)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
        return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)

            def adjusting(today, total):
                past_days = total - today
                if total > self.max_bonus_amount:
                    curent = self.max_bonus_amount - past_days
                    if curent < 0:
                        return 0
                    return curent
                else:
                    return today

            as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
            as_last_day.drop('Total', inplace=True, axis=1)
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)

            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]

        for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount', self.key_column: 'ClientID'}, inplace=True)
        for_adminka['Currency'] = 'AMD'
        for_adminka['BalanceType'] = 15
        for_adminka['OperationType'] = 57
        for_adminka['Info'] = self.campaign_name
        for_adminka["Generated Unique Id"] = None

        if isfile(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv"):
            for_adminka.to_csv(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv", index=False, mode='a',
                               header=False)
        else:
            for_adminka.to_csv(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv", index=False)

        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')


from os.path import isfile, isdir, splitext

starting_path = "Y://Retention//All//Retention All//"

if not isdir(starting_path):
    starting_path = "Z://Retention//All//Retention All//"
    if not isdir(starting_path):
        starting_path = "//srvm-totofs//TTKC//Retention//All//Retention All//"
import sys

sys.path.append(f'{starting_path}with_python//')

import os
import datetime
import pandas as pd

pd.options.mode.chained_assignment = None
import queries
import E_sports_queries as e_queries
from pyodbc import connect
from shutil import move, rmtree

today_date = datetime.datetime.today().date()
yesterday = today_date - datetime.timedelta(1)
this_year = yesterday.year
month_digit = yesterday.month
file_name_date = yesterday.strftime('%d.%m.%Y')

inbox_path = f'//nl-gen-fs//Customer Acquisition and Retention//RETENTION//Arxiv Lists//Retention Lists Results//{this_year}//{month_digit}//'

from paths import *


class P2P:
    data = pd.read_excel(f"{starting_path}with_python//P2P//p2p_data.xlsx")

    deposit_bonus = data[data['py_id'].str[:4] == 'D_01']['Bonus_ID']
    spec_game_lost_bet = data[data['py_id'].str[:4] == 'B_02']['Bonus_ID']
    spec_game_lost_bet_min_lostbet = data[data['py_id'].str[:4] == 'B_03']['Bonus_ID']
    p2p_lostbet = data[data['py_id'].str[:4] == 'B_04']['Bonus_ID']
    p2p_winbet = data[data['py_id'].str[:4] == 'B_05']['Bonus_ID']
    deposit_bonus_new_users = data[data['py_id'].str[:4] == 'D_06']['Bonus_ID']
    p2p_lost_bet_min_lostbet = data[data['py_id'].str[:4] == 'B_07']['Bonus_ID']
    all_types = (p2p_lost_bet_min_lostbet, deposit_bonus_new_users, p2p_winbet, deposit_bonus, spec_game_lost_bet,
                 spec_game_lost_bet_min_lostbet, p2p_lostbet)
    path = p2p_path

    def __init__(self, action, path=path, data=data):
        self.today_date = datetime.datetime.today().date()
        self.fin = data[data['Bonus_ID'] == action]

        self.start_date = self.fin.iloc[0]['Start date'].date()
        self.end_date = self.fin.iloc[0]['End date'].date()
        self.lasts = (self.end_date - self.start_date).days + 1
        self.count = (self.today_date - self.start_date).days
        self.campaign_name = self.fin.iloc[0]['Campaign name'].strip()
        self.bonus_type = self.fin.iloc[0]['py_id']
        self.game_name = self.fin.iloc[0]['Game name']
        self.percent = self.fin.iloc[0]['Percent'] / 100
        self.min_dep_amount = self.fin.iloc[0]['Min deposit']
        self.min_bet_amount = self.fin.iloc[0]['Min bet']
        self.min_lostbet = self.fin.iloc[0]['Min lost bet']
        self.max_bonus_amount = self.fin.iloc[0]['Max bonus']
        self.folder_date = self.start_date.strftime('%d.%m.%Y')
        self.folder_name = self.campaign_name + '_' + self.folder_date
        self.list_name = self.campaign_name.replace(' ', '_') + '_' + 'Sms_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_call = self.campaign_name.replace(' ', '_') + '_' + 'Call_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_mail = self.campaign_name.replace(' ', '_') + '_' + 'Mail_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.history_bonuses = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Bonuses//{self.campaign_name}_{self.folder_date}.csv"
        self.original_bonus = f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'
        self.original_list = f'{path}//{self.folder_name}//{self.list_name}'
        self.history_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//SMS//{self.list_name}"
        self.original_call_list = f'{path}//{self.folder_name}//{self.list_name_call}'
        self.history_call_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Call//{self.list_name_call}"
        self.original_mail_list = f'{path}//{self.folder_name}//{self.list_name_mail}'
        self.history_mail_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Mail//{self.list_name_mail}"
        self.lower_bound = 200
        self.key_column = 'CasinoID'

        self.users = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name}', usecols=[self.key_column])
        try:
            users2 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_call}', usecols=[self.key_column])
            self.users = pd.concat([self.users, users2], ignore_index=True)
        except:
            pass
        try:
            users3 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_mail}', usecols=[self.key_column])

            self.users = pd.concat([self.users, users3], ignore_index=True)
        except:
            pass
        self.users.drop_duplicates(subset=[self.key_column], inplace=True)
        self.users.dropna(subset=[self.key_column], inplace=True)

    def check_validity(self, path=path):
        if self.count > self.lasts:
            print(f'{self.campaign_name} ավարտվել է {self.end_date}-ին')

            if isfile(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'):
                move(self.original_bonus, self.history_bonuses)
                try:
                    move(self.original_list, self.history_lists)
                except:
                    pass
                try:
                    move(self.original_call_list, self.history_call_lists)
                except:
                    pass
                try:
                    move(self.original_mail_list, self.history_mail_lists)
                except:
                    pass
                rmtree(f'{path}//{self.folder_name}')
            return False
        elif self.count <= 0:
            print(f'{self.campaign_name} սկսվելու է {self.start_date}-ին')
            return False
        return True

    def get_data(self):
        while True:
            try:
                cnxn = connect(
                    "Driver={ODBC Driver 17 for SQL Server};"
                    "Server=DWH;"
                    "Database=dwOper;"
                    "Trusted_Connection=yes;"
                )
                break
            except:
                print("Trying again")
                continue
        if self.bonus_type[4] == '2' and self.bonus_type[-1] == '1':
            query_start_date = datetime.datetime.strftime(self.start_date - datetime.timedelta(1),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        else:

            query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                          format='%Y-%m-%d') + ' 20:00:00'

        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        if self.bonus_type[2:4] == '01':
            dep_q = queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = queries.get_p2p_bets(query_start_date, query_end_date, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets
        elif self.bonus_type[2:4] == '06':
            dep_q = queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = queries.get_bets(query_start_date, query_end_date, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets
        elif self.bonus_type[2:4] == '02':
            bet_q = queries.lost_bet_spec_game(query_start_date, query_end_date, self.percent, self.game_name,
                                               self.min_bet_amount, self.count)
        elif self.bonus_type[2:4] == '03':
            bet_q = queries.lost_bet_spec_game_min_lostbet(query_start_date, query_end_date, self.percent,
                                                           self.game_name, self.min_lostbet, self.count)
        elif self.bonus_type[2:4] == '04':
            bet_q = queries.p2p_lostbet(query_start_date, query_end_date, self.percent, self.count)
        elif self.bonus_type[2:4] == '05':
            bet_q = queries.p2p_winbet(query_start_date, query_end_date, self.percent, self.min_bet_amount, self.count)
        elif self.bonus_type[2:4] == '07':
            bet_q = queries.lost_bet_spec_game_min_lostbet(query_start_date, query_end_date, self.percent,
                                                           self.game_name, self.min_lostbet, self.count)
        # new bonus here
        else:
            raise Exception('Unknown bonus type:', self.bonus_type)
        bets = pd.read_sql(bet_q, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets, deposit=None):
        if self.bonus_type[4] == '2':  # դեպոզիտից ա թե չէ
            deposit.drop_duplicates('Base_UserID', inplace=True)
            bets.set_index('Base_UserID', inplace=True)
            joined = deposit.join(bets, 'Base_UserID')

            clean_bet = joined[joined['OrderDate'] >= joined['Date']][['Base_UserID', f'Bet_amount_{self.count}']]
            grouped_bets = clean_bet.groupby('Base_UserID').sum()
            all_in_one = deposit.join(grouped_bets, 'Base_UserID').drop('Date', axis=1)
            filtered_by_worth = all_in_one.loc[
                all_in_one[f'Bet_amount_{self.count}'] >= all_in_one[f'Dep_amount_{self.count}']]

            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(deposit):
                    if deposit >= 1000 and deposit <= 5000:
                        return '1000-5000'
                    elif deposit > 5000 and deposit <= 10000:
                        return '5001-10000'
                    elif deposit > 10000 and deposit <= 20000:
                        return '10001-20000'
                    elif deposit > 20000:
                        return 20001

                filtered_by_worth['Ranges'] = filtered_by_worth[f'Dep_amount_{self.count}'].apply(set_ranges)
                joined = filtered_by_worth.join(for_join, 'Ranges')
                joined[f'Bonus_amount_{self.count}'] = (joined[f'Dep_amount_{self.count}'] * joined['Percent']) / 100
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
            elif self.bonus_type[0] == 'D':
                filtered_by_worth[f'Bonus_amount_{self.count}'] = filtered_by_worth[
                                                                      f'Dep_amount_{self.count}'] * self.percent
                joined = filtered_by_worth.copy()

            joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                          upper=self.max_bonus_amount)
            joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
            joined.set_index('Base_UserID', inplace=True)

            all_in = self.users.join(joined, self.key_column)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            return all_in

        elif self.bonus_type[4] == '1':  # մենակ bet-ով ակցիա
            bets.set_index('Base_UserID', inplace=True)
            all_in = self.users.join(bets, self.key_column)
            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(bet):
                    if bet >= 1000 and bet <= 10000:
                        return '1000-10000'
                    elif bet > 10000 and bet <= 20000:
                        return '10001-20000'
                    elif bet > 20000 and bet <= 60000:
                        return '20001-60000'
                    elif bet > 60000:
                        return 60001

                all_in['Ranges'] = all_in[f'Bet_{self.count}'].apply(set_ranges)
                joined = all_in.join(for_join, 'Ranges')

                joined[f'Bonus_amount_{self.count}'] = (joined[f'Bet_{self.count}'] * joined['Percent']) / 100
                joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                              upper=self.max_bonus_amount)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].fillna(0)
                return joined
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(lower=self.lower_bound,
                                                                                   upper=self.max_bonus_amount)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
            return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            if self.bonus_type[-1] == '2':
                # print(f'{action} action gives bonuses twice')
                def adjusting(today, total):
                    past_days = total - today
                    if total > self.max_bonus_amount:
                        curent = self.max_bonus_amount - past_days
                        if curent < 0:
                            return 0
                        return curent
                    else:
                        return today

                as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                    lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
                as_last_day.drop('Total', inplace=True, axis=1)
                as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            elif self.bonus_type[-1] == '1':
                # print(f'"{action}" action gives bonuses once')
                as_last_day.loc[as_last_day['Total'] != 0, f'Adj_bonus_{self.count}'] = 0
                as_last_day.drop('Total', inplace=True, axis=1)
            else:
                raise Exception('No such type bonus: ', self.bonus_type)
            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]

        for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount', self.key_column: 'ClientID'}, inplace=True)
        for_adminka['Currency'] = 'AMD'
        for_adminka['BalanceType'] = 15
        for_adminka['OperationType'] = 57
        for_adminka['Info'] = self.campaign_name
        for_adminka["Generated Unique Id"] = None
        if isfile(f"{adminka_file_path}P2P_{file_name_date}.csv"):
            for_adminka.to_csv(f"{adminka_file_path}P2P_{file_name_date}.csv", index=False, mode='a', header=False)
        else:
            for_adminka.to_csv(f"{adminka_file_path}P2P_{file_name_date}.csv", index=False)
        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')


class Analytics:
    analytic_month_name = (datetime.datetime.today() - datetime.timedelta(30)).strftime('%B')
    analytic_month_digit = (datetime.datetime.today() - datetime.timedelta(30)).month
    analytic_month_year_digit = (datetime.datetime.today() - datetime.timedelta(30)).year
    sms_result_file = fr'{starting_path}with_python\Analytics\Joined_dexatel_{analytic_month_name}.csv'
    sms_result_path = f'{starting_path}SMS results//{this_year}//{analytic_month_digit}//'

    def __init__(self, product=None, coworker=None):
        if product:
            self.product = product
            self.db_path = f'{starting_path}with_python//{product}//Ակցիաներ//History//{Analytics.analytic_month_year_digit}//{Analytics.analytic_month_digit}//'
            self.bonus_files_path = f'{self.db_path}Bonuses//'
            self.lists_path = f'{self.db_path}Lists//SMS//'
            self.sms_result_destination = f'{self.db_path}Analytics//SMS_results//'
            self.bonus_destination = f'{self.db_path}Analytics//users_bonus//'
        elif coworker:
            self.lists_path = rf'{starting_path}with_python\Analytics\{coworker}\\'
            self.sms_result_destination = rf'{starting_path}with_python\Analytics\{coworker}_SMS_result\\'

    def merging_sms_results(self, dexatel_file_name=None, nikita_web=None, nikita_mark=None):
        print('Creating SMS unique file')
        if dexatel_file_name:
            dexatel = pd.read_excel(Analytics.sms_result_path + f"{dexatel_file_name}.xlsx", header=1)
            dexatel_filtered = dexatel[dexatel['Status'].isin(['DELIVRD', 'SENT'])][
                ['External label for pack of EDR', 'Recipient', 'Message text']]
            dexatel_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            dexatel_filtered = pd.DataFrame()

        if nikita_web:
            nikita_web = pd.read_excel(Analytics.sms_result_path + f"{nikita_web}.xlsx")
            nikita_web_filtered = nikita_web[nikita_web['Status'].isin(['Delivered', 'Transmitted'])][
                ['Group', 'MSISDN', 'Message']]
            nikita_web_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            nikita_web_filtered = pd.DataFrame()

        if nikita_mark:
            nikita_2 = pd.read_excel(Analytics.sms_result_path + f"{nikita_mark}.xlsx")
            nikita_2_filtered = nikita_2[nikita_2['Status'].isin(['Delivered', 'Transmitted'])][
                ['Group', 'MSISDN', 'Message']]
            nikita_2_filtered.columns = ['Campaign ID', 'PhoneNumber', 'Message']
        else:
            nikita_2_filtered = pd.DataFrame()

        sms_result_new = pd.concat([dexatel_filtered, nikita_web_filtered, nikita_2_filtered], ignore_index=True)

        sms_result_new['Campaign ID'] = sms_result_new['Campaign ID'].str.strip()
        sms_result_new.to_csv(Analytics.sms_result_file, index=False)
        self.sms_result_new = sms_result_new

    def get_sms_data(self):
        print('Getting SMS data')
        if isfile(Analytics.sms_result_file):
            self.sms_result_new = pd.read_csv(Analytics.sms_result_file)
            return self.sms_result_new
        else:
            dexatel_file_name = input('Input dexatel file name: ')
            nikita_web = input('Input nikita web file name: ')
            nikita_mark = input('Input nikita mark file name: ')
            self.merging_sms_results(dexatel_file_name, nikita_web, nikita_mark)
            return pd.read_csv(Analytics.sms_result_file, dtype={'PhoneNumber': 'int'})

    def lists_with_sms_results(self):

        sms_files = os.listdir(self.lists_path)

        for file in sms_files:

            try:
                df = pd.read_excel(self.lists_path + file)
            except ValueError as v_err:
                df = pd.read_csv(self.lists_path + file)

            df.dropna(subset=['PartnerUserId'], inplace=True)

            try:
                campaign_id = df.iloc[0]['Campaign ID'].strip()  # .split('_')[0]
            except:
                print('Campaign ID is empty:', file)
                continue

            filtered_sms = self.sms_result_new[self.sms_result_new['Campaign ID'] == campaign_id][
                ['PhoneNumber', 'Message']]

            if filtered_sms.shape[0] == 0:
                print(f'Can not find {campaign_id} from {file} data in sms list')
                continue

            merged = pd.merge(df, filtered_sms, left_on='Contact ID (tel/mail)', right_on='PhoneNumber', how='left')

            column_na_count = merged['PhoneNumber'].isna().sum()
            if merged.shape[0] == column_na_count:
                print(f'{campaign_id} from {file} does not find anybody')
                continue

            merged.drop('Message', axis=1, inplace=True)
            merged['Customer comment'] = merged['PhoneNumber'].copy()
            merged.drop('PhoneNumber', axis=1, inplace=True)
            merged.to_excel(self.sms_result_destination + splitext(file)[0] + '.xlsx', index=False)

    def calculate_bonus(self):

        data = pd.read_excel(f"{starting_path}with_python//{self.product}//{self.product}_data.xlsx",
                             sheet_name=Analytics.analytic_month_name)

        if self.product == 'Sport':
            col_names = ['SportID', 'Total', 'ID']
        elif self.product == 'P2P':
            col_names = ['CasinoID', 'Total', 'ID']
        data.dropna(subset=['Campaign name'], inplace=True)

        for index, row in data.iterrows():

            bonus_id = row['Bonus_ID']
            #         if bonus_id in skip_action:
            #             continue
            file_name = row['Campaign name'].strip() + '_' + str(row['Start date'].date().strftime('%d.%m.%Y')) + '.csv'
            #         bonus_file = pd.read_csv(f'{raw_files_path}Bonuses//'+file_name,usecols=['CasinoID','Total'])
            bonus_file = pd.read_csv(f'{self.bonus_files_path}//' + file_name)
            adj_bonus_cols = [col for col in bonus_file.columns if 'Adj_bonus' in col]
            bonus_file['Total'] = bonus_file[adj_bonus_cols].sum(axis=1)
            without_0 = bonus_file[bonus_file['Total'] != 0]
            without_0['ID'] = bonus_id

            try:
                without_0 = without_0[col_names]
            except:
                col_names = ['CasinoID', 'Total', 'ID']
                without_0 = without_0[col_names]

            # without_0.rename(columns={'SportID':'CasinoID'})

            if index == 0:
                final_bonus_file = without_0
            else:
                final_bonus_file = pd.concat([final_bonus_file, without_0], ignore_index=True)

        final_bonus_file.to_csv(f'{self.bonus_destination}bonus.csv', index=False)


class E_Sports(P2P):
    data = pd.read_excel(f"{starting_path}with_python//E_Sports//Campaigns.xlsx")

    deposit_bonus = data[data['py_id'].str[:4] == 'D_01']['Bonus_ID'].values
    ggr_bonus = data[data['py_id'].str[:4] == 'B_02']['Bonus_ID'].values
    all_types = (deposit_bonus, ggr_bonus)

    path = esports_path

    def __init__(self, action, path=path, data=data):

        super().__init__(action, path, data)

        self.min_odd = self.fin.iloc[0]['Min odd']
        self.freespin_id = self.fin.iloc[0]['Freespin ID']
        self.one_time = self.fin.iloc[0]['One time']
        self.path = path
        if self.one_time:
            self.count = (self.today_date.date() - self.end_date).days
            self.lasts = 1

    def get_data(self):
        cnxn = connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DWH;"
            "Database=dwOper;"
            "Trusted_Connection=yes;"
        )
        if (self.bonus_type[4] == '2' and self.bonus_type[-1] == '1') or self.one_time:

            query_start_date = datetime.datetime.strftime(self.start_date - datetime.timedelta(1),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        else:
            query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                          format='%Y-%m-%d') + ' 20:00:00'
        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        if self.bonus_type[2:4] == '01':
            dep_q = e_queries.get_deposit(query_start_date, query_end_date, self.min_dep_amount, self.count)
            bet_q = e_queries.get_esports_bets(query_start_date, query_end_date, self.min_odd, self.count)
            deposits = pd.read_sql(dep_q, cnxn)
            bets = pd.read_sql(bet_q, cnxn)
            cnxn.close()
            return deposits, bets

        elif self.bonus_type[2:4] == '02':

            bet_q = e_queries.get_esport_ggr(query_start_date, query_end_date, self.min_odd, self.min_bet_amount,
                                             self.percent, self.count)  # ,tuple(self.users[self.key_column].values)

        # new bonus here

        else:
            raise Exception('Unknown bonus type:', self.bonus_type)
        bets = pd.read_sql(bet_q, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets, deposit=None):
        if self.bonus_type[4] == '2':  # դեպոզիտից ա թե չէ
            deposit.drop_duplicates('Base_UserID', inplace=True)
            bets.set_index('Base_UserID', inplace=True)
            joined = deposit.join(bets, 'Base_UserID')

            clean_bet = joined[joined['OrderDate'] >= joined['Date']][['Base_UserID', f'Bet_amount_{self.count}']]
            grouped_bets = clean_bet.groupby('Base_UserID').sum()
            all_in_one = deposit.join(grouped_bets, 'Base_UserID').drop('Date', axis=1)
            filtered_by_worth = all_in_one.loc[
                all_in_one[f'Bet_amount_{self.count}'] >= all_in_one[f'Dep_amount_{self.count}']]

            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(deposit):
                    if deposit >= 1000 and deposit <= 5000:
                        return '1000-5000'
                    elif deposit > 5000 and deposit <= 10000:
                        return '5001-10000'
                    elif deposit > 10000 and deposit <= 20000:
                        return '10001-20000'
                    elif deposit > 20000:
                        return 20001

                filtered_by_worth['Ranges'] = filtered_by_worth[f'Dep_amount_{self.count}'].apply(set_ranges)
                joined = filtered_by_worth.join(for_join, 'Ranges')
                joined[f'Bonus_amount_{self.count}'] = (joined[f'Dep_amount_{self.count}'] * joined['Percent']) / 100
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
            elif self.bonus_type[0] == 'D':
                if self.freespin_id:
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = self.freespin_id
                else:
                    filtered_by_worth[f'Bonus_amount_{self.count}'] = filtered_by_worth[
                                                                          f'Dep_amount_{self.count}'] * self.percent
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = filtered_by_worth[f'Bonus_amount_{self.count}'].clip(
                        lower=self.lower_bound, upper=self.max_bonus_amount)
                    filtered_by_worth[f'Adj_bonus_{self.count}'] = filtered_by_worth[f'Adj_bonus_{self.count}'].apply(
                        round)
                filtered_by_worth.set_index('Base_UserID', inplace=True)
                joined = filtered_by_worth.copy()

            all_in = self.users.join(joined, self.key_column)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            return all_in

        elif self.bonus_type[4] == '1':  # մենակ bet-ով ակցիա
            bets.set_index('Base_UserID', inplace=True)
            all_in = self.users.join(bets, self.key_column)
            if self.bonus_type[0] == 'R':
                for_join = self.fin[['Ranges', 'Percent']].set_index('Ranges')

                def set_ranges(bet):
                    if bet >= 1000 and bet <= 10000:
                        return '1000-10000'
                    elif bet > 10000 and bet <= 20000:
                        return '10001-20000'
                    elif bet > 20000 and bet <= 60000:
                        return '20001-60000'
                    elif bet > 60000:
                        return 60001

                all_in['Ranges'] = all_in[f'Bet_{self.count}'].apply(set_ranges)
                joined = all_in.join(for_join, 'Ranges')

                joined[f'Bonus_amount_{self.count}'] = (joined[f'Bet_{self.count}'] * joined['Percent']) / 100
                joined[f'Adj_bonus_{self.count}'] = joined[f'Bonus_amount_{self.count}'].clip(lower=self.lower_bound,
                                                                                              upper=self.max_bonus_amount)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].apply(round)
                joined.drop(columns=['Ranges', 'Percent'], inplace=True)
                joined[f'Adj_bonus_{self.count}'] = joined[f'Adj_bonus_{self.count}'].fillna(0)
                return joined
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(lower=self.lower_bound,
                                                                                   upper=self.max_bonus_amount)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
            all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
            return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            if self.bonus_type[-1] == '2':
                # print(f'{action} action gives bonuses twice')
                def adjusting(today, total):
                    past_days = total - today
                    if total > self.max_bonus_amount:
                        curent = self.max_bonus_amount - past_days
                        if curent < 0:
                            return 0
                        return curent
                    else:
                        return today

                as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                    lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
                as_last_day.drop('Total', inplace=True, axis=1)
                as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)
            elif self.bonus_type[-1] == '1':
                # print(f'"{action}" action gives bonuses once')
                as_last_day.loc[as_last_day['Total'] != 0, f'Adj_bonus_{self.count}'] = 0
                as_last_day.drop('Total', inplace=True, axis=1)
            else:
                raise Exception('No such type bonus: ', self.bonus_type)
            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
            for_adminka.rename(columns={self.key_column: 'ClientID'}, inplace=True)

        if self.freespin_id:
            for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'BonusId'}, inplace=True)
            for_adminka['Note'] = self.campaign_name

        else:

            for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount'}, inplace=True)
            for_adminka['Currency'] = 'AMD'
            for_adminka['BalanceType'] = 15
            for_adminka['OperationType'] = 57
            for_adminka['Info'] = self.campaign_name

        for_adminka["Generated Unique Id"] = None

        filename_to_be_saved = f"{adminka_file_path}E_Sports_{file_name_date}"
        if self.freespin_id:
            filename_to_be_saved += '_freespin'

        if isfile(filename_to_be_saved + ".csv"):
            mode = 'a'
            header = False
        else:
            mode = 'w'
            header = True

        for_adminka.to_csv(f"{filename_to_be_saved}.csv", index=False, mode=mode, header=header)
        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')


class BOG:
    path = bog_path

    def __init__(self, campaign_name, lower_bound, max_bonus_amount, path=path):

        self.start_date = datetime.datetime(2022, 9, 13)
        self.end_date = datetime.datetime(2022, 9, 15)
        self.lasts = (self.end_date - self.start_date).days + 1
        self.count = (self.today_date - self.start_date).days
        self.campaign_name = campaign_name
        self.percent = 100 / 100
        self.max_bonus_amount = max_bonus_amount
        self.folder_date = self.start_date.strftime('%d.%m.%Y')
        self.folder_name = self.campaign_name + '_' + self.folder_date
        self.list_name = self.campaign_name.replace(' ', '_') + '_' + 'Sms_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_call = self.campaign_name.replace(' ', '_') + '_' + 'Call_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'
        self.list_name_mail = self.campaign_name.replace(' ', '_') + '_' + 'Mail_' + self.start_date.strftime(
            '%d.%m.%Y') + '.xlsx'

        # self.history_bonuses =f"{path}//History//{self.start_date.year}//{self.start_date.month}//Bonuses//{self.campaign_name}_{self.folder_date}.csv"
        self.original_bonus = f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv'
        self.original_list = f'{path}//{self.folder_name}//{self.list_name}'
        # self.history_lists =  f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//SMS//{self.list_name}"
        self.original_call_list = f'{path}//{self.folder_name}//{self.list_name_call}'
        # self.history_call_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Call//{self.list_name_call}"
        self.original_mail_list = f'{path}//{self.folder_name}//{self.list_name_mail}'
        # self.history_mail_lists = f"{path}//History//{self.start_date.year}//{self.start_date.month}//Lists//Mail//{self.list_name_mail}"
        # self.lower_bound = lower_bound
        self.key_column = 'CasinoID'
        self.users = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name}', usecols=[self.key_column])

        try:
            users2 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_call}', usecols=[self.key_column])
            self.users = pd.concat([self.users, users2], ignore_index=True)
        except:
            pass
        try:
            users3 = pd.read_excel(f'{path}//{self.folder_name}//{self.list_name_mail}', usecols=[self.key_column])

            self.users = pd.concat([self.users, users3], ignore_index=True)
        except:
            pass
        self.users.drop_duplicates(subset=[self.key_column], inplace=True)
        self.users.dropna(subset=[self.key_column], inplace=True)

    def get_data(self):
        cnxn = connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DWH;"
            "Database=dwOper;"
            "Trusted_Connection=yes;"
        )

        query_start_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(2),
                                                      format='%Y-%m-%d') + ' 20:00:00'
        query_end_date = datetime.datetime.strftime(self.today_date - datetime.timedelta(1),
                                                    format='%Y-%m-%d') + ' 19:59:59'

        ggr_query = f"""
        SELECT u.Base_UserID
        , SUM(CASE 
                WHEN o.CalculationDate_DT < '2021-03-01'
                    THEN CASE 
                            WHEN cg.GameProviderID IN (48, 10)
                                AND o.TypeId = 1
                                AND o.OrderDate > '2020-05-20 06:45:00'
                                THEN o.OrderAmount * o.Odds / 100
                            ELSE (o.OrderAmount - o.WinAmount)
                            END
                ELSE CASE 
                        WHEN cg.GameProviderID IN (48, 10)
                            AND o.TypeId = 1
                            AND o.OrderDate > '2020-05-20 06:45:00'
                            THEN o.OrderAmount * o.Odds / 100
                        WHEN cg.GameProviderID IN (48, 10)
                            AND o.TypeId IN (5, 8, 18, 33)
                            THEN (o.OrderAmount - o.WinAmount)
                        WHEN cg.GameProviderID NOT IN (48, 10)
                            THEN (o.OrderAmount - o.WinAmount)
                        ELSE 0
                        END
                END) * {self.percent} Bonus_{self.count} 
    FROM casino.orders o
    INNER JOIN VIEW_PlatformPartnerUsers_TotogamingAm u ON u.UserID = o.UserID
    INNER JOIN C_Game cg ON cg.GameID = o.GameID
    INNER JOIN C_GameProvider p ON p.GameProviderID= cg.GameProviderID


    WHERE u.UserTypeID <> 1
        AND o.OrderDate >= '{query_start_date}'
        AND o.OrderDate < '{query_end_date}'
        AND o.OrderStateID NOT IN (1, 4, 7)
        AND o.OperationTypeID = 3
        AND CASE 
            WHEN cg.GameProviderID IN (48, 10)
                AND o.CalculationDate_DT < '2021-03-01'
                THEN o.TypeId
            ELSE 0
            END IN (0, 1, 5, 8, 18, 33)
        and p.GameProviderID = 25
    GROUP BY u.Base_UserID
    """
        # having SUM(CASE
        #             WHEN o.CalculationDate_DT < '2021-03-01'
        #                 THEN CASE
        #                         WHEN cg.GameProviderID IN (48, 10)
        #                             AND o.TypeId = 1
        #                             AND o.OrderDate > '2020-05-20 06:45:00'
        #                             THEN o.OrderAmount * o.Odds / 100
        #                         ELSE (o.OrderAmount - o.WinAmount)
        #                         END
        #             ELSE CASE
        #                     WHEN cg.GameProviderID IN (48, 10)
        #                         AND o.TypeId = 1
        #                         AND o.OrderDate > '2020-05-20 06:45:00'
        #                         THEN o.OrderAmount * o.Odds / 100
        #                     WHEN cg.GameProviderID IN (48, 10)
        #                         AND o.TypeId IN (5, 8, 18, 33)
        #                         THEN (o.OrderAmount - o.WinAmount)
        #                     WHEN cg.GameProviderID NOT IN (48, 10)
        #                         THEN (o.OrderAmount - o.WinAmount)
        #                     ELSE 0
        #                     END
        #             END) * {self.percent} > {self.lower_bound}

        bets = pd.read_sql(ggr_query, cnxn)
        cnxn.close()
        return bets

    def preprocessing(self, bets):

        bets.set_index('Base_UserID', inplace=True)
        all_in = self.users.join(bets, self.key_column)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Bonus_{self.count}'].clip(upper=self.max_bonus_amount)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].fillna(0)
        all_in[f'Adj_bonus_{self.count}'] = all_in[f'Adj_bonus_{self.count}'].apply(round)
        return all_in

    def make_excels(self, prp_data, path=path):

        if self.count > 1:
            # print(f'Calculating {count} day bonus for {action}')
            bonus_data = pd.read_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv')
            as_last_day = bonus_data.join(prp_data.set_index(self.key_column), on=self.key_column,
                                          rsuffix=f'_{self.count}')
            adj_bonus_cols = [col for col in as_last_day.columns if 'Adj_bonus' in col]
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)

            def adjusting(today, total):
                past_days = total - today
                if total > self.max_bonus_amount:
                    curent = self.max_bonus_amount - past_days
                    if curent < 0:
                        return 0
                    return curent
                else:
                    return today

            as_last_day[f'Adj_bonus_{self.count}'] = as_last_day.apply(
                lambda x: adjusting(x[f'Adj_bonus_{self.count}'], x['Total']), axis=1)
            as_last_day.drop('Total', inplace=True, axis=1)
            as_last_day['Total'] = as_last_day[adj_bonus_cols].sum(axis=1)

            as_last_day.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = as_last_day[as_last_day[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]
        else:
            # print(f'Calculating {count} day bonus for {action}')
            prp_data.to_csv(f'{path}//{self.folder_name}//{self.campaign_name}_{self.folder_date}.csv', index=False)
            # print(f'"{campaign_name}" bonus list is ready')
            for_adminka = prp_data[prp_data[f'Adj_bonus_{self.count}'] > 0][
                [self.key_column, f'Adj_bonus_{self.count}']]

        for_adminka.rename(columns={f'Adj_bonus_{self.count}': 'Amount', self.key_column: 'ClientID'}, inplace=True)
        for_adminka['Currency'] = 'AMD'
        for_adminka['BalanceType'] = 15
        for_adminka['OperationType'] = 57
        for_adminka['Info'] = self.campaign_name
        for_adminka["Generated Unique Id"] = None

        if isfile(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv"):
            for_adminka.to_csv(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv", index=False, mode='a',
                               header=False)
        else:
            for_adminka.to_csv(f"{adminka_file_path}{self.campaign_name}_{file_name_date}.csv", index=False)

        print(f'"{self.campaign_name}_{self.start_date}" adminka file is ready')
