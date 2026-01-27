import os, time, math, csv
import requests
import pandas as pd
import pytz

from io import StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv


load_dotenv()
SHEETS=os.getenv("SHEETS")

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = "https://api.dhan.co/v2"
INTRADAY_URL = f"{BASE_URL}/charts/intraday"
FNO_MASTER_URL = f"{BASE_URL}/instrument/NSE_FNO"

HEADERS = {
    "access-token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}
OPTIONSHEADERS = {
    "access-token": ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept":"application/json"
}

IST = pytz.timezone("Asia/Kolkata")
REPLAY_DATE = "2026-01-22"
ENGINE_START_TIME = "10:01:00"
ENGINE_END_TIME   = "15:20:00"

engine_time = IST.localize(datetime.strptime(f"{REPLAY_DATE} {ENGINE_START_TIME}", "%Y-%m-%d %H:%M:%S")).replace(tzinfo=None)
engine_end  = IST.localize(datetime.strptime(f"{REPLAY_DATE} {ENGINE_END_TIME}", "%Y-%m-%d %H:%M:%S")).replace(tzinfo=None)

def fetch_option_candles(securityid:str,interval:int):

    payload = {
        "securityId": str(securityid),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "interval": f"{interval}",
        "fromDate": f"2026-01-22 10:00:00",
        "toDate": f"2026-01-22 10:02:00",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", []),
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(df)
    return df


def change_time(engine_time):
    print("time before", engine_time)
    engine_time = engine_time - timedelta(minutes=1)
    print("time after", engine_time)
    return engine_time

def send_to_sheet(sheet, row):
    payload = {
        "sheet": sheet,
        "row": row
    }
    try:
        r = requests.post(SHEETS, json=payload, timeout=5)
        r.raise_for_status()
    except Exception as e:
        print("Log failed:", e)

def log_event(event, info=""):
    send_to_sheet("engine_log", [
        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        event,
        info
    ])

def log_trade(symbol, side, entry, exit, qty, pnl, reason):
    send_to_sheet("trades", [
        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        symbol,
        side,
        entry,
        exit,
        qty,
        pnl,
        reason
    ])



log_event("ENGINE_START", "Test engine started")
log_event("CE_SIGNAL", "Index broke bottom")

log_trade("CE","SELL",210.5,180.2,50,(210.5 - 180.2) * 50,"SL_TRAIL")
 