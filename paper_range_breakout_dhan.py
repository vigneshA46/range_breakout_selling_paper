import os, time, math, csv
import requests
import pandas as pd
import pytz

from io import StringIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ======================
# ENV + CONFIG
# ======================

load_dotenv()

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

TRADE_DATE  = "2026-01-22"
TRADE_START = "09:15:00"
TRADE_END   = "15:30:00"

NIFTY_SECURITY_ID = "13"
STRIKE_GAP = 50

QTY = 50
DAY_STOP = -2500

REPLAY_DATE = "2026-01-22"
ENGINE_START_TIME = "10:01:00"
ENGINE_END_TIME   = "15:20:00"

# ======================
# YOUR FUNCTIONS (USED)
# ======================

# fetch_instruments()

def fetch_instruments():

    print("downloading FNO master")

    r = requests.get(FNO_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text), header=None, low_memory=False)

    df.columns = [
        "EXCH_ID","SEGMENT","SECURITY_ID","ISIN","INSTRUMENT",
        "UNDERLYING_SECURITY_ID","UNDERLYING_SYMBOL","SYMBOL_NAME",
        "DISPLAY_NAME","INSTRUMENT_TYPE","SERIES","LOT_SIZE",
        "SM_EXPIRY_DATE","STRIKE_PRICE","OPTION_TYPE","TICK_SIZE",
        "EXPIRY_FLAG","BRACKET_FLAG","COVER_FLAG","ASM_GSM_FLAG",
        "ASM_GSM_CATEGORY","BUY_SELL_INDICATOR",
        "BUY_CO_MIN_MARGIN_PER","BUY_CO_SL_RANGE_MAX_PERC",
        "BUY_CO_SL_RANGE_MIN_PERC","BUY_BO_MIN_MARGIN_PER",
        "BUY_BO_PROFIT_RANGE_MAX_PERC","BUY_BO_PROFIT_RANGE_MIN_PERC",
        "MTF_LEVERAGE","RESERVED"
    ]

    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    return df

# fetch_index_candles and index candles()


def fetch_option_candles(security_id, interval):
    payload = {
        "securityId": f"{security_id}",
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "interval": str(interval),
        "fromDate": f"{TRADE_DATE} {TRADE_START}",
        "toDate": f"{TRADE_DATE} {TRADE_END}",
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

    return df

def fetch_index_candles(interval, engine_time):
    payload = {
        "securityId": NIFTY_SECURITY_ID,
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": str(interval),
        "fromDate": f"{engine_time - timedelta(minutes=1)}",
        "toDate": f"{engine_time + timedelta(minutes=1)}",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    if "timestamp" not in data:
        return pd.DataFrame()

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

# pick_itm8()


def pick_itm8(df, atm):
    df = df.copy()

    # remove junk header row
    df = df[df["EXCH_ID"] != "EXCH_ID"]

    # only nifty index options
    df = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == "NIFTY")
    ]

    df = df.sort_values("SM_EXPIRY_DATE")

    if df.empty:
        raise ValueError("❌ No NIFTY OPTIDX rows found in FNO master")

    expiry = df.iloc[0]["SM_EXPIRY_DATE"]
    df = df[df["SM_EXPIRY_DATE"] == expiry]

    ce_strike = atm - 8 * STRIKE_GAP
    pe_strike = atm + 8 * STRIKE_GAP

    ce_df = df[(df["OPTION_TYPE"] == "CE") & (df["STRIKE_PRICE"] == ce_strike)]
    pe_df = df[(df["OPTION_TYPE"] == "PE") & (df["STRIKE_PRICE"] == pe_strike)]

    if ce_df.empty:
        raise ValueError(f"❌ No CE found for strike {ce_strike}")

    if pe_df.empty:
        raise ValueError(f"❌ No PE found for strike {pe_strike}")

    ce = ce_df.iloc[0]
    pe = pe_df.iloc[0]

    print(f"🎯 Picked CE Strike: {ce_strike}")
    print(f"🎯 Picked PE Strike: {pe_strike}")

    return ce, pe


# ======================
# HELPERS
# ======================

def round_to_gap(x, gap=50):
    return int(round(x / gap) * gap)


def fetch_option_candle(security_id,engine_time):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "interval": "1",
        "fromDate": f"{engine_time - timedelta(minutes=1)}",
        "toDate": f"{engine_time + timedelta(minutes=1)}",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    if "timestamp" not in data:
        return None

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    print(df)
    return df.iloc[-1]

# ======================
# RANGE MARKING
# ======================

def mark_open_range():
    payload = {
        "securityId": "13",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "fromDate": f"{TRADE_DATE} 09:54:00",
        "toDate": f"{TRADE_DATE} 10:00:00",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()
    print(data)

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", []),
    })
    IST = pytz.timezone("Asia/Kolkata")
    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(df)
    row = df.iloc[-1]

    top = max(row.open, row.close)
    bottom = min(row.open, row.close)
    atm = round_to_gap(row.close)

    print(f"📌 RANGE MARKED | TOP={top} BOTTOM={bottom} ATM={atm}")

    return top, bottom, atm


# ======================
# CSV LOGGER
# ======================

def init_logs():
    if not os.path.exists("trades.csv"):
        with open("trades.csv","w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","symbol","side","entry","exit","qty","pnl","reason"])

    if not os.path.exists("engine_log.csv"):
        with open("engine_log.csv","w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","event","info"])


def log_event(event, info=""):
    with open("engine_log.csv","a",newline="") as f:
        csv.writer(f).writerow([datetime.now(IST), event, info])


def log_trade(symbol, side, entry, exit, qty, pnl, reason):
    with open("trades.csv","a",newline="") as f:
        csv.writer(f).writerow([datetime.now(IST), symbol, side, entry, exit, qty, pnl, reason])


# ======================
# POSITION ENGINE
# ======================

def new_position(symbol, price):
    return {
        "symbol": symbol,
        "entry": price,
        "sl": price - 15,
        "tsl": price - 30,
        "active": False,      # tsl activated?
        "trail_steps": 0,     # how many 10pt trails done
        "qty": QTY
    }

def manage_sl_tsl(pos, ltp):
    """
    Returns (exit, reason)
    """

    entry = pos["entry"]

    # ---------- ACTIVATE TSL ----------
    
    if not pos["active"]:
        if ltp <= entry - 30:
            pos["active"] = True
            pos["trail_steps"] = 1

            pos["sl"]  = entry + 15 - 10
            pos["tsl"] = entry + 30 - 10

            print(f"🟢 {pos['symbol']} TSL Activated | SL={pos['sl']} TSL={pos['tsl']}")

    # ---------- TRAIL ----------
    if pos["active"]:
        favorable = entry - ltp
        steps = int((favorable - 30) // 10) + 1

        if steps > pos["trail_steps"]:
            diff = (steps - pos["trail_steps"]) * 10
            pos["sl"]  -= diff
            pos["tsl"] -= diff
            pos["trail_steps"] = steps

            print(f"🔁 {pos['symbol']} Trailed | SL={pos['sl']} TSL={pos['tsl']}")

    # ---------- SL HIT ----------
    if pos["active"] and ltp >= pos["sl"]:
        return True, "SL_HIT"

    return False, None



# ======================
# MAIN ENGINE
# ======================

def run():
    print("run running succesfully")
    exit_flag = False
    reason = None

    def wait_real_minute():
        now = datetime.now()
        sleep_sec = 60 - now.second
        if sleep_sec <= 0:
            sleep_sec = 1
        time.sleep(sleep_sec + 1)

    engine_time = IST.localize(datetime.strptime(f"{REPLAY_DATE} {ENGINE_START_TIME}", "%Y-%m-%d %H:%M:%S")).replace(tzinfo=None)
    engine_end  = IST.localize(datetime.strptime(f"{REPLAY_DATE} {ENGINE_END_TIME}", "%Y-%m-%d %H:%M:%S")).replace(tzinfo=None)


    init_logs()
    log_event("ENGINE_START")

    #print("⏳ Waiting for 10:01...")
    #while datetime.now(IST).time() < datetime.strptime("10:01","%H:%M").time():
    #    time.sleep(1)
 
    top, bottom, atm = mark_open_range()

    fno = fetch_instruments()
    ce, pe = pick_itm8(fno, atm)

    CE_ID = ce.SECURITY_ID
    PE_ID = pe.SECURITY_ID
    print("security id of CE and PE")
    print("CE",CE_ID,"PE",PE_ID)

    ce_pos = None
    pe_pos = None

    pending_ce = False
    pending_pe = False

    signal_ce_time = None
    signal_pe_time = None


    total_pnl = 0

    print("🚀 Engine Running...")

    last_idx_time = None


    print("🚀 Engine Running (REPLAY MODE)...")

    last_idx_time = None

    while engine_time <= engine_end:

    # ================= REAL CLOCK WAIT =================
        wait_real_minute()

        print("⏱ REAL:", datetime.now(), "ENGINE:", engine_time)

        from_dt = engine_time - timedelta(minutes=1)
        to_dt   = engine_time

    # ================= FETCH =================

        idx_df = fetch_index_candles(1,engine_time)

        if idx_df.empty:
            engine_time += timedelta(minutes=1)
            continue

        idx = idx_df.iloc[-1]

        if last_idx_time == idx.datetime:
            engine_time += timedelta(minutes=1)
            continue

        last_idx_time = idx.datetime

        ce_c = fetch_option_candle(CE_ID,engine_time)
        pe_c = fetch_option_candle(PE_ID,engine_time)

        avg = (idx.open + idx.high + idx.low + idx.close) / 4
        # ================= SL / TSL =================

        # ---- CE ----
        if ce_pos:
            exit_flag, reason = manage_sl_tsl(ce_pos, ce_c.close)
        if exit_flag:
            pnl = (ce_pos["entry"] - ce_c.close) * QTY
            log_trade("CE","SELL",ce_pos["entry"],ce_c.close,QTY,pnl,reason)
            total_pnl += pnl
            ce_pos = None
 
        # ---- PE ----
        if pe_pos:
            exit_flag, reason = manage_sl_tsl(pe_pos, pe_c.close)
        if exit_flag:
            pnl = (pe_pos["entry"] - pe_c.close) * QTY
            log_trade("PE","SELL",pe_pos["entry"],pe_c.close,QTY,pnl,reason)
            total_pnl += pnl
            pe_pos = None


        # ================= ENTRY SIGNAL =================

        if ce_pos is None and not pending_ce:
            if idx.close < bottom and avg < bottom and avg > idx.close:
                pending_ce = True
                signal_ce_time = idx.datetime
                log_event("CE_SIGNAL")

        if pe_pos is None and not pending_pe:
            if idx.close > top and avg > top and avg < idx.close:
                pending_pe = True
                signal_pe_time = idx.datetime
                log_event("PE_SIGNAL")

        # ================= EXECUTE PENDING =================

        if pending_ce and ce_pos is None and idx.datetime > signal_ce_time:
            ce_pos = new_position("CE", ce_c.close)
            pending_ce = False
            signal_ce_time = None
            log_event("CE_ENTRY", ce_c.close)

        if pending_pe and pe_pos is None and idx.datetime > signal_pe_time:
            pe_pos = new_position("PE", pe_c.close)
            pending_pe = False
            signal_pe_time = None
            log_event("PE_ENTRY", pe_c.close)
 
        # ================= INDEX EXIT =================

        if ce_pos and idx.close > bottom:
            pnl = (ce_pos["entry"] - ce_c.close) * QTY
            log_trade("CE","SELL",ce_pos["entry"],ce_c.close,QTY,pnl,"INDEX_EXIT")
            total_pnl += pnl
            ce_pos = None

        if pe_pos and idx.close < top:
            pnl = (pe_pos["entry"] - pe_c.close) * QTY
            log_trade("PE","SELL",pe_pos["entry"],pe_c.close,QTY,pnl,"INDEX_EXIT")
            total_pnl += pnl
            pe_pos = None

        open_pnl = 0

        if ce_pos:
            open_pnl += (ce_pos["entry"] - ce_c.close) * QTY

        if pe_pos:
            open_pnl += (pe_pos["entry"] - pe_c.close) * QTY

        net_mtm = total_pnl + open_pnl
 

        # ================= DAY STOP =================

        if net_mtm <= DAY_STOP:
            log_event("DAY_STOP", net_mtm)
            break
 
        engine_time += timedelta(minutes=1)

 
    print("Engine Stopped")
    log_event("ENGINE_STOP")
 

if __name__ == "__main__":
    print("file running succesfully")
    while True:
        run()
 