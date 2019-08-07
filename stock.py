#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 19:02:22 2019

@author: feifanhe
"""

import pandas as pd

class Environment:
    
    def load_data(self, filename):
        self.data = pd.read_excel(filename)
        self.data.rename(columns = {
                '證券代碼':'code', 
                '年月日':'timestamp', 
                '開盤價(元)':'open', 
                '最高價(元)':'high', 
                '最低價(元)':'low', 
                '收盤價(元)':'close'}, inplace=True)
        self.data['code'] = self.data['code'].str.split(' ', n = 1, expand = True)[0].astype(int)
        self.group_data = self.data.groupby('code')
        
        # 各檔股票開盤價時序資料
        self.open_price = self.data.pivot(index = 'timestamp', columns = 'code', values = 'open')
        
    def get_price(self, symbol):
        price = self.group_data.get_group(symbol).set_index('timestamp').drop('code', axis = 1)
        return price
    
    def get_stock_table(self, session):
        # 產生各檔股票的持有部位時序資料
        date_table = pd.DataFrame(self.data['timestamp'].unique(), columns=['timestamp'])
        date_table = date_table.set_index('timestamp')
        stock_table = session.actions.pivot(index = 'date', columns = 'code', values = 'amount').fillna(0)
        session_date = date_table[stock_table.index[0]:stock_table.index[-1]]
        return pd.concat([session_date, stock_table.reindex(session_date.index)], axis = 1).fillna(0)

        
#%%
# data part
env = Environment()
env.load_data('1601_0050.xlsx')
data = env.data


#%%
class Session:
    
    def __init__(self):
        return

    def load_action(self, filename):
        # action json
        self.actions = pd.read_json(filename, convert_dates=True)

    
#%%
# 讀取寫在 JSON 檔案的 action
session = Session()
session.load_action('action.json')
actions = session.actions
position_table = env.get_stock_table(session)

#%%

# 計算損益
# TODO: merge 到 env
gains_list = []
returns_list = []
gains_rate_list = []

position_pool = dict()
targets = position_table.columns.values
price_table = dict()

for target in targets:
    price_table[target] = env.get_price(target)
    
for index, data in position_table.iterrows():
    total_gains = 0
    total_returns = 0
    total_cost = 0
    for target in targets:
        current_state = [0, 0, 0, 0] # position, cost, gains, returns
        
        if target in position_pool.keys():
            current_state = position_pool[target]
            
        position = data[target]
        current_position = current_state[0]
        delta_position = position - current_position
        open_price = price_table[target].loc[index]['open']
        #close_price = price_table[target].loc[index]['close']
        cost = current_state[1]
        gains = current_state[2]
        returns = current_state[3]
        if delta_position > 0:
            cost = ( cost * current_position + open_price * delta_position ) / position
        if position > 0:
            gains = (open_price - cost) * position
        else:
            gains = 0
        if delta_position < 0:
            returns = returns - delta_position * (open_price - cost)
            
        position_pool[target] = [position, cost, gains, returns]
        total_returns += returns
        total_gains += gains
        total_cost += cost * position
        
    print(index, total_gains, total_returns)
    gains_list.append(total_gains)
    returns_list.append(total_returns)
    if (total_cost != 0):
        gains_rate_list.append(100 * total_gains / total_cost)
    else:
        gains_rate_list.append(0)

#%%   
# 產生損益結果
zippedList =  list(zip(gains_list, gains_rate_list, returns_list))
returns_table = pd.DataFrame(
        zippedList,
        columns = ['gains', 'gains_rate(%)', 'returns'],
        index = position_table.index)
