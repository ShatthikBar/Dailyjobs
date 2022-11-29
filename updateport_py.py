# %%
## 
import pandas as pd
import datetime

import paramiko
import io
import os 
import numpy as np

from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from xbbg import blp
import pandas as pd
from dateutil.relativedelta import relativedelta

import requests
import json
import xmltodict

import sys


# %%
engine = create_engine(URL(
        account = 'QCOHCUV-MM41421',
        user = 'SHATTHIK',
        password = 'Shattiker1',
        database = 'PORT',
        schema = 'CASH',
        warehouse = 'COMPUTE_WH'
    ))

Username=r'NorselabAPI'
Password=r'N0rseL@bAS2022'


# %%
def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter

# %%
if is_notebook():
    calc_type = "INTRADAY"
else:
    calc_type = sys.argv[1]


# %%
stamp_time = pd.Timestamp(datetime.datetime.now(),tz='UTC')

if calc_type == "INTRADAY":
    sett_day = datetime.date.today()+ datetime.timedelta(days=-1)
    TRADES = pd.read_sql("SELECT * FROM TRADE.FLOW.TRADE_APPROVED_FX WHERE settlement_date >= '{0}' or settlement_date_spot >= '{0}' or settlement_date_fwd >= '{0}'".format(str(datetime.date.today())),con =engine)
if calc_type == "EOD":
    sett_day = datetime.date.today()
    TRADES = pd.read_sql("SELECT * FROM TRADE.FLOW.TRADE_APPROVED_FX WHERE settlement_date > '{0}' or settlement_date_spot > '{0}' or settlement_date_fwd > '{0}'".format(str(datetime.date.today())),con =engine)


# %%
BB_PORT = pd.read_sql("SELECT AS_OF_DATE,SECURITY_ID,TOTAL_UNITS FROM PORT.CASH.PORTFOLIO WHERE AS_OF_DATE = '{0}'".format(str(sett_day)),con =engine)
BB_PORT["as_of_date"] = datetime.date.today()

# %%
BB_PORT

# %%
#### SPLIT_POS




BOND_TRADES = TRADES[TRADES["contract_type"] == "Bond"].copy()
SPOT_TRADES = TRADES[(TRADES["contract_type"] == "SPOT") | (TRADES["contract_type"] == "SWAP")].copy()
FWD_TRADES = TRADES[(TRADES["contract_type"] == "FWD")|(TRADES["contract_type"] == "SWAP")].copy()
CASH_TRADES = TRADES[(TRADES["instrument"] == "Cash")].copy()

SPOT_TRADES = SPOT_TRADES[SPOT_TRADES.settlement_date_spot>= datetime.date.today()]

######

def CASH_TRADE(CASH_TRADES,BB_PORT):
    BB_PORT = BB_PORT.copy()

    CASH_TRADES = CASH_TRADES[CASH_TRADES.settlement_date == datetime.date.today()]
    CASH_TRADES = CASH_TRADES[["trade_date","settlement_date","trade_type","isin","amount","nett","cur"]].copy()
    CASH_TRADES["nett"] =CASH_TRADES["amount"]
    CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","amount"] = CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","amount"]*-1
    CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","nett"] = CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","nett"]*-1
    CASH_TRADES_agg = CASH_TRADES.groupby(["cur"]).agg({"amount":"sum","nett":"sum"}).reset_index()


    ##BOND


    for index, row in CASH_TRADES.iterrows():
        BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"] = row["nett"]+BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"]

    return BB_PORT





def BOND_TRADE(BOND_TRADES,BB_PORT):
    BB_PORT = BB_PORT.copy()

    BOND_TRADES = BOND_TRADES[["trade_date","settlement_date","trade_type","isin","amount","nett","cur"]].copy()
    BOND_TRADES.loc[BOND_TRADES["trade_type"] == "SELL","amount"] = BOND_TRADES.loc[BOND_TRADES["trade_type"] == "SELL","amount"]*-1
    BOND_TRADES.loc[BOND_TRADES["trade_type"] == "BUY","nett"] = BOND_TRADES.loc[BOND_TRADES["trade_type"] == "BUY","nett"]*-1
    BOND_TRADES_agg = BOND_TRADES.groupby(["isin","cur"]).agg({"amount":"sum","nett":"sum"}).reset_index()


    ##BOND


    for index, row in BOND_TRADES_agg.iterrows():
        if row["isin"] in list(BB_PORT["security_id"]):
            BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"] = row["amount"]+BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"]
        else:
            df_app = pd.DataFrame([{'as_of_date': datetime.date.today(),
        'security_id': row["isin"],
        'total_units': row["amount"]}])
            BB_PORT = pd.concat([BB_PORT,df_app])

        BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"] = row["nett"]+BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"]

    return BB_PORT

    
def SPOT_TRADE(SPOT_TRADES,BB_PORT):
    ###FX SPOT
    BB_PORT = BB_PORT.copy()

    SPOT_TRADES = SPOT_TRADES[["trade_date","settlement_date_spot","buy_cur_spot","sell_cur_spot","buy_amount_spot","sell_amount_spot"]].copy()
    SPOT_TRADES["sell_amount_spot"] = SPOT_TRADES["sell_amount_spot"]*-1


    spot_buyleg = SPOT_TRADES[["buy_cur_spot","buy_amount_spot"]]
    spot_sellleg = SPOT_TRADES[["sell_cur_spot","sell_amount_spot"]]

    spot_buyleg.columns = spot_sellleg.columns = ["isin","nett"]

    spot_trans = pd.concat([spot_buyleg,spot_sellleg]).groupby(["isin"]).nett.sum().reset_index()

    for index, row in spot_trans.iterrows():
        BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"] = row["nett"]+BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"]

    return BB_PORT


###FX FWD
REF_RATES = {"NOK":'YCMM0044 Index',"EUR":'YCMM0085 Index',"USD":'YCMM0021 Index',"SEK":"YCMM0045 Index","DKK":"YCMM0042 Index"}
IBOR_CURVE = blp.bds(tickers=REF_RATES["NOK"], flds=['CURVE_TENOR_RATES'])

def diff_month(start, end):
    return 360*(end.year - start.year)\
               + 30*(end.month - start.month -1)\
               + max(0, 30 - start.day)\
               + min(30, end.day)

def M2M(trade,IBOR_CURVE):

    value_date = trade["settlement_date_fwd"]
    
    if trade['buy_cur_fwd'] == "NOK":
        BASE = trade['sell_cur_fwd']
        NOK_K = trade['buy_cur_fwd']
        ttype = "SELL"
        f0 = trade["buy_amount_fwd"]/trade["sell_amount_fwd"]
        amt = trade["buy_amount_fwd"]
        qty = amt/f0
        
    else: 
        BASE = trade['buy_cur_fwd']
        NOK_K = trade['sell_cur_fwd']
        ttype = "BUY"
        f0 = trade["sell_amount_fwd"]/trade["buy_amount_fwd"]
        amt = trade["sell_amount_fwd"]
        qty = amt/f0

    

    

    def addDay(td,ad):
        if ad[-1] == "W":
            dv = relativedelta(weeks=+int(ad[0:-1])) + datetime.date.today()
        if ad[-1] == "M":
            dv = relativedelta(months=+int(ad[0:-1])) + datetime.date.today()
        if ad[-1] == "Y":
            dv = relativedelta(years=+int(ad[0:-1])) + datetime.date.today()

        return(dv)

    ticker_fwd = '{0:}/{1:} {2:} Curncy'.format(BASE,NOK_K,value_date.strftime('%m/%d/%y'))

    fwd = blp.bdp(ticker_fwd,["PX_BID",'PX_ASK'])



    ticker = '{0:}/{1:} Curncy'.format(BASE,NOK_K)
    spot = blp.bdp(ticker,["PX_LAST","PX_BID",'PX_ASK'])

    
    IBOR_CURVE["tenor_dates"] = IBOR_CURVE.tenor.apply(lambda x: addDay(IBOR_CURVE.last_update.iloc[0],x))
    daily_rates = IBOR_CURVE[["tenor_dates","mid_yield"]].set_index("tenor_dates").reindex(pd.date_range(start=IBOR_CURVE.tenor_dates.min(), end=IBOR_CURVE.tenor_dates.max(), freq='1D')).interpolate(method='linear') 
    daily_rates.index.freq=None  

    
    nday = diff_month(pd.to_datetime(value_date),pd.to_datetime(trade["settlement_date_fwd"]))

    if ttype == "SELL":
        PL = ((f0-fwd[{"SELL":"px_bid","BUY":"px_last"}[ttype]].iloc[0])*qty)/(1+daily_rates.loc[pd.to_datetime(value_date)].iloc[0]/100*(nday/360))
    if ttype == "BUY":
        PL = ((fwd[{"SELL":"px_bid","BUY":"px_last"}[ttype]].iloc[0]-f0)*qty)/(1+daily_rates.loc[pd.to_datetime(value_date)].iloc[0]/100*(nday/360))


    r_d = {'as_of_date': datetime.date.today(),
    'security_id': f'{trade["buy_cur_fwd"]}/{trade["sell_cur_fwd"]}',
    'total_units': PL}

    #print(ticker_fwd)

    return r_d


def FWD_TRADE(FWD_TRADES,BB_PORT):
    BB_PORT = BB_PORT.copy()

    fwd_buyleg = FWD_TRADES[["buy_cur_fwd","buy_amount_fwd"]].copy()
    fwd_sellleg = FWD_TRADES[["sell_cur_fwd","sell_amount_fwd"]].copy()
    fwd_sellleg["sell_amount_fwd"] = fwd_sellleg["sell_amount_fwd"]*-1


    fwd_buyleg.columns = fwd_sellleg.columns = ["isin","nett"]
    fwd_trans = pd.concat([fwd_buyleg,fwd_sellleg]).groupby(["isin"]).nett.sum().reset_index()


    if fwd_trans.shape[0]>0:

        fwd_pos = []
        for i in range(0,FWD_TRADES.shape[0]):
            fwd_pos.append(M2M(FWD_TRADES.iloc[i],IBOR_CURVE))

        FWD_LINES = pd.DataFrame(fwd_pos)
        FWD_LINES= FWD_LINES.groupby(["as_of_date","security_id"]).total_units.sum().reset_index()
    else:
        FWD_LINES = pd.DataFrame(columns=["as_of_date","security_id","total_units"])


    BB_PORT = pd.concat([BB_PORT,FWD_LINES])
    BB_PORT = BB_PORT.groupby(["as_of_date","security_id"]).total_units.sum().reset_index()

    return BB_PORT

def send_to_BB(BB_PORT_3):
    BB_PORT_3.to_excel("port_bb.xlsx",index=False)



    hostname = "sftp.bloomberg.com"
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
    ssh.connect(hostname, port = 22 ,username="u1251988031", password="BloombergNorselab1=")


    ftp = ssh.open_sftp()
    files = ftp.listdir()
    ftp.put("port_bb.xlsx", "port_bb.xlsx")
    ssh.close()


def send_to_db(BB_PORT_3):
    
    BB_PORT_3 = BB_PORT_3.copy()
    BB_PORT_3["TIMESTAMP"] =  stamp_time
    BB_PORT_3.columns = BB_PORT_3.columns.str.upper()
    BB_PORT_3.to_sql("portfolio_trade",con = engine,if_exists="append",index=False,method=pd_writer)


    



# %%

BB_PORT_0 = CASH_TRADE(CASH_TRADES,BB_PORT)
BB_PORT_1 = BOND_TRADE(BOND_TRADES,BB_PORT_0)
BB_PORT_2 = SPOT_TRADE(SPOT_TRADES,BB_PORT_1)
BB_PORT_3 = FWD_TRADE(FWD_TRADES,BB_PORT_2)


# %%
BB_PORT

# %%
BB_PORT_3

# %%


# %%
send_to_BB(BB_PORT_3)
print("PORT SENT TO BB")

# %%
##CASH LEDGER

##
def callstamdata(Username,Password,bdy):
    url = "https://feed.stamdata.com/services/IStamdataDataService/getDataForIdentifiers" 

    

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    res = requests.post(
    url, 
    auth=(Username, Password),
    data  = json.dumps(bdy),headers=headers
    )

    obj   =xmltodict.parse(res.json())

    return obj

def get_intday(yr,dd):

    
    d = dd.split("/")[1].upper()

    if "360" in d:
         day_dem = 360
         
    if "365" in d:
         day_dem = 365

    if "ACT" in d:
         day_dem = pd.Timestamp(yr.year, 12, 31).dayofyear


    return day_dem


#####CHASH LEDGER
def CASH_FLOW(CASH_TRADES):
    


    CASH_TRADES = CASH_TRADES[["trade_date","settlement_date","trade_type","isin","amount","nett","cur"]].copy()
    CASH_TRADES["nett"] =CASH_TRADES["amount"]
    CASH_TRADES["isin"] =CASH_TRADES["cur"]
    CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","amount"] = CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","amount"]*-1
    CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","nett"] = CASH_TRADES.loc[CASH_TRADES["trade_type"] == "OUT","nett"]*-1

    CASH_CL = CASH_TRADES.groupby(["cur","settlement_date"]).nett.sum().reset_index()
    CASH_CL_DET = CASH_TRADES.groupby(["cur","isin","settlement_date"]).nett.sum().reset_index()
    CASH_CL_DET["trade_type"] = "CASHBOOKING"

    return CASH_CL,CASH_CL_DET


    ##BOND


def BOND_FLOW(BOND_TRADES):
    BOND_TRADES_CL = BOND_TRADES.copy()
    BOND_TRADES_CL.loc[BOND_TRADES_CL["trade_type"] == "BUY","nett"] = BOND_TRADES_CL.loc[BOND_TRADES_CL["trade_type"] == "BUY","nett"]*-1
    BOND_CL = BOND_TRADES_CL.groupby(["cur","settlement_date"]).nett.sum().reset_index()
    BOND_CL_DET = BOND_TRADES_CL.groupby(["cur","isin","settlement_date"]).nett.sum().reset_index()
    BOND_TRADES_sett = BOND_TRADES_CL.groupby(["isin"]).settlement_date.min().to_dict()
    BOND_CL_DET["trade_type"] = "BOND_TRADE"
    return BOND_CL,BOND_CL_DET,BOND_TRADES_sett

###cuopons

def INT_FLOW(BB_PORT_3,BOND_TRADES_sett):

    SECS = BB_PORT_3[BB_PORT_3.security_id.apply(lambda x: len(x) == 12)]
    bdy = [{"PrimaryKey":i,"table": 0}for i in SECS.security_id]

    if SECS.shape[0] > 0:

        obj = callstamdata(Username,Password,bdy)

        All_intpay = []
        All_intpay_det = []

        for bd in obj["database"]["table_Issue"]["Issue"]:

            MaturityPrice = float(bd["MaturityPrice"])

            CUP = pd.DataFrame(bd["table_IssueTermDate"]["IssueTermDate"])
            CUP["InterestCoupon_20160302"] = CUP["InterestCoupon_20160302"].ffill()

            CUP["total_units"] = SECS[SECS.security_id == bd["ISIN"]]["total_units"].iloc[0]
            CUP["days_denom"] = [get_intday(i,j) for i,j in zip(pd.to_datetime(CUP.TermDate),CUP.AccruedInterestDayCountConvention)]

            CUP["int_pay"] = CUP["total_units"]*((CUP["InterestCoupon_20160302"].astype(float))/100)*CUP["DaysInPeriod"].astype(int)/CUP["days_denom"].astype(int)
            CUP["red_pay"] = 0

            CUP.loc[CUP.Installment.astype(float) > 0,"red_pay"] = CUP.loc[CUP.Installment.astype(float) > 0,"total_units"]*(MaturityPrice/100)

            CUP["pay"]= CUP["int_pay"]+CUP["red_pay"]
            CUP["Currency"] = bd["Currency"]

            CUP = CUP[pd.to_datetime(CUP.TermDate) >= pd.to_datetime(BOND_TRADES_sett.get(bd["ISIN"],BB_PORT["as_of_date"].min()))]

            

            All_intpay.append(CUP[["Currency","TermDate","pay"]])
            All_intpay_det.append(CUP[["Currency","ISIN","TermDate","pay"]])

        INT_TRADES = pd.concat(All_intpay).groupby(["Currency","TermDate"]).sum().reset_index()
        INT_TRADES.columns = ["cur","settlement_date","nett"]

        INT_TRADES_DET = pd.concat(All_intpay_det)
        INT_TRADES_DET.columns = ["cur","ISIN","settlement_date","nett"]
    else:
        INT_TRADES = pd.DataFrame(columns= ["cur","settlement_date","nett"])
        INT_TRADES_DET = pd.DataFrame(columns= ["cur","ISIN","settlement_date","nett"])
    INT_TRADES_DET["trade_type"] = "COUPON"

    return INT_TRADES,INT_TRADES_DET


def SPOT_FLOW(SPOT_TRADES):

    SPOT_TRADES_CL = SPOT_TRADES.copy()
    SPOT_TRADES_CL["sell_amount_spot"] = SPOT_TRADES_CL["sell_amount_spot"]*-1
    SPOT_TRADES_BUY = SPOT_TRADES_CL[["buy_cur_spot","settlement_date_spot","buy_amount_spot"]].copy()
    SPOT_TRADES_SELL = SPOT_TRADES_CL[["sell_cur_spot","settlement_date_spot","sell_amount_spot"]].copy()
    SPOT_TRADES_BUY.columns = SPOT_TRADES_SELL.columns = ["cur","settlement_date","nett"]
    SPOT_CL = pd.concat([SPOT_TRADES_BUY,SPOT_TRADES_SELL]).groupby(["cur","settlement_date"]).nett.sum().reset_index()
    SPOT_CL_DET = pd.concat([SPOT_TRADES_BUY,SPOT_TRADES_SELL])
    SPOT_CL_DET["trade_type"] = "SPOT_TRADE"
    SPOT_CL_DET["ISIN"] = ''
    

    return SPOT_CL,SPOT_CL_DET

def FWD_FLOW(FWD_TRADES):   
    
    FWD_TRADES_BUY = FWD_TRADES[["buy_cur_fwd","settlement_date_fwd","buy_amount_fwd"]].copy()
    FWD_TRADES_SELL = FWD_TRADES[["sell_cur_fwd","settlement_date_fwd","sell_amount_fwd"]].copy()
    FWD_TRADES_SELL["sell_amount_fwd"] = FWD_TRADES_SELL["sell_amount_fwd"]*-1

    FWD_TRADES_BUY.columns = FWD_TRADES_SELL.columns = ["cur","settlement_date","nett"]

    FWD_CL = pd.concat([FWD_TRADES_BUY,FWD_TRADES_SELL]).groupby(["cur","settlement_date"]).nett.sum().reset_index()
    FWD_CL_DET = pd.concat([FWD_TRADES_BUY,FWD_TRADES_SELL])
    FWD_CL_DET["trade_type"] = "FWD_TRADE"
    FWD_CL_DET["ISIN"] = ''

    return FWD_CL,FWD_CL_DET



def gen_CASHLEDGE(CASH_CL,BOND_CL,SPOT_CL,FWD_CL,port_dt,INT_TRADES):


      ALL_CL = pd.concat([CASH_CL,BOND_CL,SPOT_CL,FWD_CL,port_dt,INT_TRADES]).groupby(["cur","settlement_date"]).nett.sum().reset_index()


      tickers = list(set(['{0:}/NOK Curncy'.format(i) for i in ALL_CL.cur]))
      spot = blp.bdp(tickers,["PX_BID","PX_MID",'PX_ASK'])
      spot["cur"] =  [i.split("/")[0] for i in spot.index]

      cur_dict = spot[["cur","px_mid"]].copy().set_index("cur").to_dict()["px_mid"]

      total_cl = []
      for index, row in ALL_CL.iterrows():
         total_cl.append(cur_dict.get(row["cur"])*row["nett"])


      ALL_CL["total"] = total_cl
      ALL_CL.settlement_date = pd.to_datetime(ALL_CL.settlement_date).dt.date
      ALL_CL.columns = ALL_CL.columns.str.upper()

      


      return ALL_CL

def gen_CASH_DET(CASH_CL_DET,BOND_CL_DET,INT_TRADES_DET,SPOT_CL_DET,FWD_CL_DET):
    CASH_CL_DET.columns = CASH_CL_DET.columns.str.upper()
    BOND_CL_DET.columns = BOND_CL_DET.columns.str.upper()
    INT_TRADES_DET.columns = INT_TRADES_DET.columns.str.upper()
    SPOT_CL_DET.columns = SPOT_CL_DET.columns.str.upper()
    FWD_CL_DET.columns = FWD_CL_DET.columns.str.upper()
    CASH_DET = pd.concat([CASH_CL_DET,BOND_CL_DET,INT_TRADES_DET,SPOT_CL_DET,FWD_CL_DET])
    CASH_DET.SETTLEMENT_DATE = pd.to_datetime(CASH_DET.SETTLEMENT_DATE).dt.date
    return CASH_DET

def tranform_cl(ALL_CL):
    ALL_CL = ALL_CL.groupby(["CUR","SETTLEMENT_DATE"]).agg({"NETT":"sum","TOTAL":"sum"}).reset_index()
    CASH_LEDGER = ALL_CL.pivot(index="CUR",columns="SETTLEMENT_DATE",values="NETT").fillna(0)
    CASH_LEDGER_tot = ALL_CL.pivot(index="CUR",columns="SETTLEMENT_DATE",values="NETT").fillna(0)
    CASH_LEDGER.loc["TOTAL"] = CASH_LEDGER_tot.sum(0)


    CASH_LEDGER.columns = pd.to_datetime(CASH_LEDGER.columns)
    CASH_LEDGER = CASH_LEDGER[CASH_LEDGER.columns.sort_values()]
    CASH_LEDGER.columns = [i.date() for i in CASH_LEDGER.columns]

    CASH_LEDGER = CASH_LEDGER.cumsum(1).reset_index()
    CASH_LEDGER.columns = [str(i).upper() for i in CASH_LEDGER.columns]
    return CASH_LEDGER



# %%
port_dt = pd.read_sql("SELECT SECURITY_ID,AS_OF_DATE,TOTAL_UNITS FROM PORT.CASH.PORTFOLIO WHERE AS_OF_DATE = '{0}' AND LENGTH(SECURITY_ID) = 3".format(str(sett_day)),con =engine)
port_dt["as_of_date"] = datetime.date.today()
port_dt.columns = ["cur","settlement_date","nett"]

CASH_CL,CASH_CL_DET = CASH_FLOW(CASH_TRADES)
BOND_CL,BOND_CL_DET,BOND_TRADES_sett = BOND_FLOW(BOND_TRADES)
INT_TRADES,INT_TRADES_DET = INT_FLOW(BB_PORT_3,BOND_TRADES_sett)
SPOT_CL,SPOT_CL_DET = SPOT_FLOW(SPOT_TRADES)
FWD_CL,FWD_CL_DET = FWD_FLOW(FWD_TRADES)

CASH_LEDGER = gen_CASHLEDGE(CASH_CL,BOND_CL,SPOT_CL,FWD_CL,port_dt,INT_TRADES)
CASH_DET = gen_CASH_DET(CASH_CL_DET,BOND_CL_DET,INT_TRADES_DET,SPOT_CL_DET,FWD_CL_DET)



CASH_LEDGER["TIMESTAMP"] =  stamp_time
CASH_DET["TIMESTAMP"] =  stamp_time


CASH_LEDGER["CALC_TYPE"] =  calc_type
CASH_DET["CALC_TYPE"] =  calc_type


# %%
CASH_LEDGER.to_sql("cashledger",con = engine,if_exists="append",index=False,method=pd_writer)
CASH_DET.to_sql("cashdetails",con = engine,if_exists="append",index=False,method=pd_writer)
print("CASHLEDGER UPDATED")

# %%
tranform_cl(CASH_LEDGER)

# %%


# %%
####hedge

def BOND_TRADE_hd(BOND_TRADES,BB_PORT):
    BB_PORT = BB_PORT.copy()

    BOND_TRADES = BOND_TRADES[["trade_date","settlement_date","trade_type","isin","amount","nett","cur"]].copy()
    BOND_TRADES.loc[BOND_TRADES["trade_type"] == "SELL","amount"] = BOND_TRADES.loc[BOND_TRADES["trade_type"] == "SELL","amount"]*-1
    BOND_TRADES.loc[BOND_TRADES["trade_type"] == "BUY","nett"] = BOND_TRADES.loc[BOND_TRADES["trade_type"] == "BUY","nett"]*-1
    BOND_TRADES_agg = BOND_TRADES.groupby(["isin","cur"]).agg({"amount":"sum","nett":"sum"}).reset_index()


    ##BOND


    for index, row in BOND_TRADES_agg.iterrows():
        if row["isin"] in list(BB_PORT["security_id"]):
            BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"] = row["amount"]+BB_PORT.loc[BB_PORT["security_id"] == row["isin"],"total_units"]
        else:
            df_app = pd.DataFrame([{'as_of_date': datetime.date.today(),
        'security_id': row["isin"],
        'total_units': row["amount"]}])
            BB_PORT = pd.concat([BB_PORT,df_app])

        #BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"] = row["nett"]+BB_PORT.loc[BB_PORT["security_id"] == row["cur"],"total_units"]

    return BB_PORT

    


###hedge###


def get_HEDGE(BB_PORT,BOND_TRADES,SPOT_TRADES,FWD_TRADES):
    BB_HD = BOND_TRADE(BOND_TRADES,BB_PORT)
    BB_HD = SPOT_TRADE(SPOT_TRADES,BB_HD)

    HEDGE_PORT = BB_HD[BB_HD.security_id.apply(lambda x:len(x)!= 7)].copy()

    bd_isin = list(HEDGE_PORT[HEDGE_PORT.security_id.apply(lambda x:len(x)== 12)].security_id)
    bd_isin = ["/ISIN/"+i for i in bd_isin]

    if len(bd_isin) > 0:
        bd_price = blp.bdp(bd_isin,["CRNCY","PX_DIRTY_BID"])
        bd_price["security_id"] = [i.split("/")[2]for i in bd_price.index]
    else:
        bd_price = pd.DataFrame([{"crncy":"NOK","px_dirty_bid":100,"security_id":"NOK"}])
    HEDGE_PORT = HEDGE_PORT.merge(bd_price,on = "security_id",how ="left")
    HEDGE_PORT.crncy = HEDGE_PORT.crncy.fillna(HEDGE_PORT.security_id)
    HEDGE_PORT.px_dirty_bid = HEDGE_PORT.px_dirty_bid.fillna(100)
    HEDGE_PORT["mval"] = HEDGE_PORT["total_units"]*HEDGE_PORT["px_dirty_bid"]/100
    PORT_EXP = HEDGE_PORT.groupby("crncy").mval.sum().reset_index()



    FWD_TRADES_BUY = FWD_TRADES[["buy_cur_fwd","settlement_date_fwd","buy_amount_fwd"]].copy()
    FWD_TRADES_SELL = FWD_TRADES[["sell_cur_fwd","settlement_date_fwd","sell_amount_fwd"]].copy()
    FWD_TRADES_SELL["sell_amount_fwd"] = FWD_TRADES_SELL["sell_amount_fwd"]*-1
    FWD_TRADES_BUY.columns = FWD_TRADES_SELL.columns = ["cur","settlement_date","nett"]
    FWD_CL = pd.concat([FWD_TRADES_BUY,FWD_TRADES_SELL]).groupby(["cur","settlement_date"]).nett.sum().reset_index()

    if FWD_CL.shape[0]>0:

        HEDGE_FWD = FWD_CL.groupby("cur").sum("nett").reset_index()
    else:
        HEDGE_FWD = pd.DataFrame(columns=["cur","nett"])




    HEDGE_FWD.columns = ["crncy","fwd"]
    TOTAL_HEDGE = PORT_EXP.merge(HEDGE_FWD,on ="crncy",how ="outer")
    TOTAL_HEDGE = TOTAL_HEDGE[TOTAL_HEDGE.crncy != "NOK"].fillna(0)
    TOTAL_HEDGE["HEDGE_RATIO"] =  1-(TOTAL_HEDGE["mval"]+TOTAL_HEDGE["fwd"])/TOTAL_HEDGE["mval"]
    TOTAL_HEDGE["HEDGE_REST"] = (TOTAL_HEDGE["mval"]+TOTAL_HEDGE["fwd"])*-1
    #TOTAL_HEDGE["DATE"] = str(datetime.date.today())
    TOTAL_HEDGE.columns = [str(i).upper() for i in TOTAL_HEDGE.columns]
    TOTAL_HEDGE = TOTAL_HEDGE.fillna(0)
    return TOTAL_HEDGE

# %%
TOTAL_HEDGE = get_HEDGE(BB_PORT,BOND_TRADES,SPOT_TRADES,FWD_TRADES)
TOTAL_HEDGE["TIMESTAMP"] = stamp_time
TOTAL_HEDGE["CALC_TYPE"] = calc_type


# %%
TOTAL_HEDGE.to_sql("hedge",con = engine,if_exists="append",index=False,method=pd_writer)
print("HEDGE UPDATED")

# %%
TOTAL_HEDGE

# %%
###NBP GET PRICE

def get_NBP_prices(ISINS):
    Username ="NBP50975PriceFeed"
    Password="N0rseL@bAS2022"

    bdy = [{"PrimaryKey":i, "table":2}for i in ISINS]

    url = "https://feed.stamdata.com/services/IStamdataDataService/getDataForIdentifiers" 



    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    res = requests.post(
    url, 
    auth=(Username, Password),
    data  = json.dumps(bdy),headers=headers
    )

        
    NBP_prices = pd.DataFrame(xmltodict.parse(res.json())["database"]["table_PricingNordicBondPricingLatest"]["PricingNordicBondPricingLatest"])
    return pd.DataFrame(NBP_prices)





# %%
##### PORTSTATS####

SECS_PORT = BB_PORT_3[BB_PORT_3.security_id.apply(lambda x: len(x)==12)].copy()
PORT_ISIN = list(SECS_PORT.security_id.apply(lambda x: "/ISIN/"+x))
BB_PORT_3["index"] = list(BB_PORT_3.security_id.apply(lambda x: "/ISIN/"+x))


DESC = blp.bdp(tickers=PORT_ISIN, flds=["SECURITY_NAME","ISSUER","INDUSTRY_SECTOR","INDUSTRY_GROUP"])
YEILD = blp.bdp(tickers=PORT_ISIN, flds=['YAS_XCCY_FIXED_COUPON_EQUIVALENT'],YAS_XCCY_FOREIGN_CURRENCY = "NOK")
NUM = blp.bdp(tickers=PORT_ISIN, flds=["MTY_YEARS_TDY","YAS_MOD_DUR","DUR_MID","UD_NET_IMPACT_SHATT_1","PX_LAST","INT_ACC"])
NUM_CAT = blp.bdp(tickers=PORT_ISIN, flds=["UD_NORSE_RAT","QUOTED_CRNCY","FIXED"])


BB_FLDS = pd.concat([DESC,YEILD,NUM,NUM_CAT],axis = 1).reset_index()
SECS_PORT["index"] = PORT_ISIN


if BB_FLDS.shape[0]==0:
    
    BB_FLDS_0 = ["SECURITY_NAME","ISSUER","INDUSTRY_SECTOR","INDUSTRY_GROUP"]+['YAS_XCCY_FIXED_COUPON_EQUIVALENT']+["MTY_YEARS_TDY","YAS_MOD_DUR","DUR_MID","UD_NET_IMPACT_SHATT_1","PX_LAST","INT_ACC"]+["UD_NORSE_RAT","QUOTED_CRNCY","FIXED"]
    BB_FLDS = pd.DataFrame(columns=list(map(str.lower,BB_FLDS_0)))
    BB_FLDS["index"] = PORT_ISIN

PORT0 = BB_PORT_3.merge(BB_FLDS,on = "index",how = "left")



tickers_cur = list(set(['{0:}/NOK Curncy'.format(i) for i in ["DKK","NOK","SEK","EUR","USD"]]))
spot = blp.bdp(tickers_cur,["PX_BID","PX_MID",'PX_ASK'])
spot["quoted_crncy"] =  [i.split("/")[0] for i in spot.index]


PORT0["INSTRUMENT"] = PORT0.security_id.apply(lambda x: {12:"security",3:"fx",7:"fwd"}.get(len(x)))
PORT0.loc[PORT0.INSTRUMENT != "security","px_last"] = 1
PORT0.loc[PORT0.INSTRUMENT == "fx","quoted_crncy"] = PORT0.loc[PORT0.INSTRUMENT == "fx","security_id"]
PORT0.loc[PORT0.INSTRUMENT == "fwd","quoted_crncy"] ="NOK"

PORT0 = PORT0.merge(spot[["px_mid","quoted_crncy"]],on = "quoted_crncy",how ="left")

PORT0["int_acc"] = PORT0["int_acc"].fillna(0)
PORT0["mval_price"] = PORT0["px_last"]+PORT0["int_acc"]
PORT0.loc[PORT0.INSTRUMENT == "security","mval_price"] = PORT0.loc[PORT0.INSTRUMENT == "security","mval_price"]/100
PORT0["mval"] = PORT0["total_units"]*PORT0["mval_price"]*PORT0["px_mid"]


PORT0["wgt"] = PORT0["mval"]/PORT0["mval"].sum()*100

PORT0["coupon_type"] = PORT0["fixed"].apply(lambda x: {"Y":"FIXED","N":"FLAOT"}.get(x))


PORT1 = PORT0[["INSTRUMENT","security_id","security_name","issuer","wgt","mval","total_units","px_last","int_acc","quoted_crncy","px_mid","ud_norse_rat","ud_net_impact_shatt_1","yas_xccy_fixed_coupon_equivalent","mty_years_tdy","yas_mod_dur","dur_mid","coupon_type","industry_sector","industry_group"]].copy()
PORT1.columns = ["INSTRUMENT","ISIN","name","issuer","wgt","mval","pos","price","int_acc","cur","cur_rate","creditrat_norse","net_impact","yield","ytm","mod_dur","dur","coupon_type","industry_sector","industry_group"]
PORT1 = PORT1.sort_values("INSTRUMENT",ascending=False)
PORT1.columns = PORT1.columns.str.upper()

NBP_p = get_NBP_prices(list(PORT1[PORT1.INSTRUMENT == "security"].ISIN))

NBP_p_rel = NBP_p[["ISIN","PriceDate","PriceBid","PriceEval","PriceAsk","Yield","Spread","Duration","ModifiedDuration","CreditDuration"]].copy()
NBP_p_rel = NBP_p_rel.set_index("ISIN")
NBP_p_rel[["PriceBid","PriceEval","PriceAsk","Yield","Spread","Duration","ModifiedDuration","CreditDuration"]] = NBP_p_rel[["PriceBid","PriceEval","PriceAsk","Yield","Spread","Duration","ModifiedDuration","CreditDuration"]].astype("float")
NBP_p_rel.columns = [i+"_NBP" for i in NBP_p_rel.columns]
NBP_p_rel = NBP_p_rel.reset_index()

PORT11 = PORT1.merge(NBP_p_rel,on ="ISIN",how ="left")

PORT11["PRICE_NBP"] = (PORT11["PriceEval_NBP"]+PORT11["INT_ACC"])/100
PORT11["PRICE_NBP"] = PORT11["PRICE_NBP"].fillna(PORT11["PRICE"])
PORT11["MVAL_NBP"] = PORT11["POS"]*PORT11["PRICE_NBP"]*PORT11["CUR_RATE"]

PORT11["TIMESTAMP"] = stamp_time
PORT11.columns = PORT11.columns.str.upper()



bank_rates = {"NOK":1,"EUR":1,"USD":1,"SEK":1,"DKK":1}

PORT11.loc[PORT11.INSTRUMENT!= "security","YIELD_NBP"] = PORT11.loc[PORT11.INSTRUMENT!= "security","CUR"].apply(lambda x: bank_rates.get(x,1)/100)
PORT11.loc[PORT11.INSTRUMENT!= "security","YIELD"] = PORT11.loc[PORT11.INSTRUMENT!= "security","CUR"].apply(lambda x: bank_rates.get(x,1))
PORT11.loc[PORT11.INSTRUMENT!= "security",['MOD_DUR', 'DUR','DURATION_NBP','MODIFIEDDURATION_NBP', 'CREDITDURATION_NBP']] = 0


# %%
PORT11["CALC_TYPE"] = calc_type
PORT11.to_sql("portfolio_trade",con = engine,if_exists="append",index=False,method=pd_writer)
print("PORTFOLIO UPDATED")

# %%
BONDS = PORT11[PORT11.INSTRUMENT == "security"].copy()
BONDS["WGT"] = BONDS["MVAL"]/BONDS["MVAL"].sum()

MVAL = PORT11.MVAL.sum().round(0)
MVAL_NBP = PORT11.MVAL_NBP.sum().round(0)

YTM = (PORT11["YIELD"]*PORT11["WGT"]/100).sum().round(2)
YTM_NBP = (PORT11["YIELD_NBP"]*PORT11["WGT"]).sum().round(2)

DUR = (PORT11["DUR"]*PORT11["WGT"]/100).sum().round(2)
DUR_NBP = (PORT11["DURATION_NBP"]*PORT11["WGT"]/100).sum().round(2)

MODDUR = (PORT11["MOD_DUR"]*PORT11["WGT"]/100).sum().round(2)
MODDUR_NBP = (PORT11["MODIFIEDDURATION_NBP"]*PORT11["WGT"]/100).sum().round(2)

CREDDUR = (PORT11["YTM"]*PORT11["WGT"]/100).sum().round(2)
CREDDUR_NBP = (PORT11["CREDITDURATION_NBP"]*PORT11["WGT"]/100).sum().round(2)

IMPACT = (BONDS["NET_IMPACT"]*BONDS["WGT"]).sum().round(2)


CASH = PORT11[PORT11["INSTRUMENT"] =="fx"].MVAL.sum()
CASH_NBP = PORT11[PORT11["INSTRUMENT"] =="fx"].MVAL_NBP.sum()
FWD = PORT11[PORT11["INSTRUMENT"] =="fwd"].MVAL_NBP.sum()
FWD_NBP = PORT11[PORT11["INSTRUMENT"] =="fwd"].MVAL_NBP.sum()


P_STATS = {"MVAL_BB":MVAL,
"MVAL_NBP":MVAL_NBP,
"CASH":CASH,
"CASH_NBP":CASH_NBP,
"FWD":FWD,
"FWD_NBP":FWD_NBP,
"YTM":YTM,
"YTM_NBP":YTM_NBP,
"DUR":DUR,
"DUR_NBP":DUR_NBP,
"MODDUR":MODDUR,
"MODDUR_NBP":MODDUR_NBP,
"CREDDUR":CREDDUR,
"CREDDUR_NBP":CREDDUR_NBP,
"IMPACT":IMPACT,
"TIMESTAMP":stamp_time,
"CALC_TYPE":calc_type}

MVAL_DF = pd.DataFrame([P_STATS])

# %%
MVAL_DF

# %%
MVAL_DF.to_sql("mval",con = engine,if_exists="append",index=False,method=pd_writer)
print("MVAL SAVED")

# %%



