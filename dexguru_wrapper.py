import os
import datetime as dt
import time
import math

from dotenv import load_dotenv

import numpy as np
import pandas as pd
from IPython.display import display

import requests, json

load_dotenv()  

class dexguru:
    
    API_KEY = os.environ.get("DEX_GURU_API_KEY")
    
    CHAINIDs  = {
    "ethereum": 1,
    "bsc": 56,
    "polygon": 137,
    "avalanche": 43114,
    "arbitrum": 42161,
    "fantom": 250,
    "celo": 42220,
    "optimism": 10,
    "metis":None #Placeholder for eventual integration
    }
    
    ADJUST_DICT = {
            "ETH":[["ethereum","0x11b815efb8f581194ae79006d24e0d814b7697f6"]],
            "BNB":[["bsc","0x16b9a82891338f9ba80e2d6970fdda79d1eb0dae"]],
            "FTM":[["fantom","0x5965e53aa80a0bcf1cd6dbdd72e6a9b2aa047410"]],
            "DAI":[["ethereum","0x6b175474e89094c44da98b954eedeac495271d0f"]]
        }
    
    def __init__(self):
        self.pair = None # [Chain, Contract]
        self.pairs_df = None #df of [Chain, Contract], Header: Symbol
        self.data = None # Data for [Chain,Contract]
        self.aggregated_data = None # Combines self.data for multiple contracts

    # Fetch first pair on dexscreener
    def get_pair(self,contract):
        url = requests.get("https://api.dexscreener.io/latest/dex/search?q="+ contract)
        data = json.loads(url.text)
        try:
            self.pair = [data['pairs'][0]['chainId'],data['pairs'][0]['pairAddress']]
        except:
            print("Empty response for: "+contract)
            return None
        return self.pair

    # Get prices for specific pair from dex.guru
    def get_prices(self,chain,address,start,end):
        print("https://api.dev.dex.guru/v1/chain/"
                           +str(self.CHAINIDs[chain])+
                           "/tokens/"
                           +str(address)+
                           "/market/history?api-key="
                           +self.API_KEY+"&begin_timestamp="+str(start)+"&end_timestamp="+str(end))      

        url = requests.get("https://api.dev.dex.guru/v1/chain/"
                           +str(self.CHAINIDs[chain])+
                           "/tokens/"
                           +str(address)+
                           "/market/history?api-key="
                           +self.API_KEY+"&begin_timestamp="+str(start)+"&end_timestamp="+str(end))      
        try:        
            self.data = pd.DataFrame(data=json.loads(url.text)['data'])        
        except:
            print("Empty response for: "+address)
            return None
        return self.data

    # Fetch pairs for multiple symbols
    def get_pair_data(self, ticker):
        pairs_df = pd.DataFrame(columns=ticker)        

        contract_list={}
        
        for i in pairs_df.columns:

            handler = self.get_pair(i)
            contract_list.update({i:[handler]})

        self.pairs_df = pd.DataFrame.from_dict(contract_list,orient="columns").dropna(axis=1)
        return self.adjust_contracts()
    
    def adjust_contracts(self):
        
        adjustment_df = pd.DataFrame(data=self.ADJUST_DICT)
        self.pairs_df.update(adjustment_df)

        return self.pairs_df
    
    def get_price_data(self,start,end):
        #Augment function to also switch between vol,prices,etc.
                
        data = pd.DataFrame(data=pd.date_range(start=start,end=end,freq="1D").rename("timestamp")).set_index("timestamp")

        for i in self.pairs_df:        
            print(self.pairs_df[i][0])      

            raw_object = self.get_prices(self.pairs_df[i][0][0],self.pairs_df[i][0][1],start,end)

            if raw_object is not None:        
                raw_data = raw_object.loc[:,["price_usd","timestamp","volume24h_usd"]]
                raw_data.timestamp = pd.to_datetime(raw_data.timestamp,unit='s')
                raw_data.timestamp = raw_data.timestamp.dt.round("d")                
                raw_data.set_index("timestamp",inplace=True)      

                #trim to start with first volume after 0
                start_vol = raw_data.volume24h_usd.loc[raw_data.volume24h_usd !=0].index
                if len(start_vol) > 0:
                    raw_data = raw_data.loc[start_vol[0]:]            
                self.aggregated_data = pd.concat([self.aggregated_data,pd.Series(raw_data["price_usd"]).rename(i)],axis=1)                   
            else:
                print("Probably empty: ",i)
                
        self.aggregated_data = self.aggregated_data.fillna(method="ffill")    
        return self.aggregated_data
    
    
guru = dexguru()

guru.get_pair_data(["WFTM","DAI"])

d = dt.date(2022,5,20) # Year, Month, Day 
time_end = int(time.mktime(d.timetuple()))
time_start = 1588723228 #earliest from dexguru

guru.get_price_data(time_start,time_end) # Uses pair_data

display(guru.aggregated_data)

guru.aggregated_data.to_pickle("history_example")